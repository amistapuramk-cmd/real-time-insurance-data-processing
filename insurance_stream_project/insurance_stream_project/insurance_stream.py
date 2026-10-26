"""
Real-Time Insurance Data Processing - simple base project
=========================================================

Pipeline:   Producer  ->  Queue (stream)  ->  Processor  ->  SQLite + Live stats
            (events)      (buffer)            validate
                                              enrich (premium / risk)
                                              fraud rules
                                              store

Run:        python insurance_stream.py --duration 30 --rate 10
Requires:   Python 3.8+ (standard library only)

To go "real" later, replace the Queue with Kafka / RabbitMQ / Kinesis and
SQLite with PostgreSQL - the processing logic stays the same.
"""

import argparse
import queue
import random
import sqlite3
import threading
import time
import uuid
from collections import defaultdict, deque

DB_FILE = "insurance.db"

# type -> (base premium rate per 1000 of coverage, coverage range)
POLICY_TYPES = {
    "health": (4.0, (10_000, 100_000)),
    "auto":   (6.0, (5_000, 50_000)),
    "home":   (3.0, (50_000, 500_000)),
    "life":   (2.5, (100_000, 1_000_000)),
}


# --------------------------------------------------------------------------
# 1. PRODUCER - simulates a live feed of insurance events
# --------------------------------------------------------------------------
class EventProducer(threading.Thread):
    def __init__(self, stream, rate, stop_event):
        super().__init__(daemon=True)
        self.stream = stream
        self.rate = rate                  # events per second
        self.stop_event = stop_event
        self.policies = []                # policies created so far

    def run(self):
        while not self.stop_event.is_set():
            self.stream.put(self._make_event())
            time.sleep(1.0 / self.rate)

    def _make_event(self):
        # Mix of events; policies must exist before claims/payments
        kind = random.choices(
            ["policy_created", "claim_filed", "payment_received"],
            weights=[25, 45, 30],
        )[0]
        if kind != "policy_created" and not self.policies:
            kind = "policy_created"

        event = {"event_id": str(uuid.uuid4())[:8], "type": kind, "ts": time.time()}

        if kind == "policy_created":
            ptype = random.choice(list(POLICY_TYPES))
            low, high = POLICY_TYPES[ptype][1]
            policy = {
                "policy_id": "POL-" + str(uuid.uuid4())[:6].upper(),
                "customer_id": "CUST-" + str(random.randint(1, 40)),
                "policy_type": ptype,
                "coverage": random.randint(low, high),
                "customer_age": random.randint(18, 80),
            }
            self.policies.append(policy)
            event.update(policy)

        elif kind == "claim_filed":
            policy = random.choice(self.policies)
            event.update({
                "claim_id": "CLM-" + str(uuid.uuid4())[:6].upper(),
                "policy_id": policy["policy_id"],
                "customer_id": policy["customer_id"],
                "amount": round(policy["coverage"] * random.uniform(0.01, 1.0), 2),
            })

        else:  # payment_received
            policy = random.choice(self.policies)
            event.update({
                "policy_id": policy["policy_id"],
                "amount": round(random.uniform(50, 800), 2),
            })

        # ~5% of events are deliberately corrupted to show validation working
        if random.random() < 0.05:
            if random.random() < 0.5:
                event.pop("policy_id", None)
            else:
                event["amount"] = -abs(event.get("amount", 100))
        return event


# --------------------------------------------------------------------------
# 2. PROCESSOR - validate, enrich, detect fraud, store
# --------------------------------------------------------------------------
class Stats:
    """Thread-safe live metrics shared with the reporter."""

    def __init__(self):
        self.lock = threading.Lock()
        self.data = defaultdict(float)
        self.latencies = deque(maxlen=500)

    def inc(self, key, value=1):
        with self.lock:
            self.data[key] += value

    def snapshot(self):
        with self.lock:
            snap = dict(self.data)
            lat = list(self.latencies)
        snap["avg_latency_ms"] = (sum(lat) / len(lat) * 1000) if lat else 0.0
        return snap


class EventProcessor(threading.Thread):
    REQUIRED = {
        "policy_created":   ["policy_id", "customer_id", "policy_type", "coverage", "customer_age"],
        "claim_filed":      ["claim_id", "policy_id", "customer_id", "amount"],
        "payment_received": ["policy_id", "amount"],
    }

    def __init__(self, stream, stats):
        super().__init__()
        self.stream = stream
        self.stats = stats
        self.recent_claims = defaultdict(deque)   # customer -> claim timestamps

    def run(self):
        # SQLite connections must be used in the thread that created them
        self.db = sqlite3.connect(DB_FILE)
        self._init_db()
        while True:
            event = self.stream.get()
            if event is None:                      # shutdown sentinel
                break
            started = time.time()
            self._process(event)
            self.stats.latencies.append(time.time() - started)
        self.db.commit()
        self.db.close()

    def _init_db(self):
        self.db.executescript("""
            DROP TABLE IF EXISTS policies;
            DROP TABLE IF EXISTS claims;
            DROP TABLE IF EXISTS payments;
            DROP TABLE IF EXISTS rejected;
            CREATE TABLE policies (policy_id TEXT PRIMARY KEY, customer_id TEXT,
                policy_type TEXT, coverage REAL, premium REAL, risk TEXT, created REAL);
            CREATE TABLE claims (claim_id TEXT PRIMARY KEY, policy_id TEXT,
                customer_id TEXT, amount REAL, fraud_score INTEGER,
                status TEXT, created REAL);
            CREATE TABLE payments (id INTEGER PRIMARY KEY AUTOINCREMENT,
                policy_id TEXT, amount REAL, created REAL);
            CREATE TABLE rejected (event_id TEXT, type TEXT, reason TEXT);
        """)

    # ---- pipeline -----------------------------------------------------
    def _process(self, event):
        error = self._validate(event)
        if error:
            self.db.execute("INSERT INTO rejected VALUES (?,?,?)",
                            (event.get("event_id"), event.get("type"), error))
            self.stats.inc("rejected")
            return

        handler = {
            "policy_created": self._handle_policy,
            "claim_filed": self._handle_claim,
            "payment_received": self._handle_payment,
        }[event["type"]]
        handler(event)
        self.stats.inc("processed")
        self.db.commit()

    def _validate(self, event):
        etype = event.get("type")
        if etype not in self.REQUIRED:
            return "unknown event type"
        for field in self.REQUIRED[etype]:
            if field not in event:
                return f"missing field: {field}"
        if "amount" in event and event["amount"] <= 0:
            return "amount must be positive"
        return None

    # ---- handlers -----------------------------------------------------
    def _handle_policy(self, e):
        rate = POLICY_TYPES[e["policy_type"]][0]
        age = e["customer_age"]
        age_factor = 1.0 if age < 30 else 1.2 if age < 50 else 1.5 if age < 65 else 2.0
        premium = round(e["coverage"] / 1000 * rate * age_factor / 12, 2)  # monthly
        risk = "low" if age_factor <= 1.0 else "medium" if age_factor <= 1.5 else "high"

        self.db.execute("INSERT OR REPLACE INTO policies VALUES (?,?,?,?,?,?,?)",
                        (e["policy_id"], e["customer_id"], e["policy_type"],
                         e["coverage"], premium, risk, e["ts"]))
        self.stats.inc("policies")
        self.stats.inc("monthly_premium_total", premium)

    def _handle_claim(self, e):
        row = self.db.execute(
            "SELECT coverage, created FROM policies WHERE policy_id=?",
            (e["policy_id"],)).fetchone()
        if not row:
            self.db.execute("INSERT INTO rejected VALUES (?,?,?)",
                            (e["event_id"], e["type"], "unknown policy"))
            self.stats.inc("rejected")
            return
        coverage, policy_created = row

        # --- simple rule-based fraud scoring (0-100) ---
        score = 0
        if e["amount"] > 0.8 * coverage:
            score += 40                      # claim close to full coverage
        if e["ts"] - policy_created < 5:
            score += 30                      # claim right after policy start
        window = self.recent_claims[e["customer_id"]]
        window.append(e["ts"])
        while window and e["ts"] - window[0] > 30:
            window.popleft()
        if len(window) >= 3:
            score += 30                      # many claims in a short window

        if score >= 60:
            status = "flagged_fraud"
        elif e["amount"] < 5000:
            status = "auto_approved"
        else:
            status = "manual_review"

        self.db.execute("INSERT OR REPLACE INTO claims VALUES (?,?,?,?,?,?,?)",
                        (e["claim_id"], e["policy_id"], e["customer_id"],
                         e["amount"], score, status, e["ts"]))
        self.stats.inc("claims")
        self.stats.inc("claims_amount_total", e["amount"])
        self.stats.inc("claims_" + status)

    def _handle_payment(self, e):
        self.db.execute("INSERT INTO payments (policy_id, amount, created) VALUES (?,?,?)",
                        (e["policy_id"], e["amount"], e["ts"]))
        self.stats.inc("payments")
        self.stats.inc("payments_total", e["amount"])


# --------------------------------------------------------------------------
# 3. REPORTER - live console dashboard
# --------------------------------------------------------------------------
class Reporter(threading.Thread):
    def __init__(self, stats, stream, interval, stop_event):
        super().__init__(daemon=True)
        self.stats, self.stream = stats, stream
        self.interval, self.stop_event = interval, stop_event

    def run(self):
        while not self.stop_event.wait(self.interval):
            self.print_report()

    def print_report(self):
        s = self.stats.snapshot()
        g = lambda k: s.get(k, 0)
        print("\n" + "=" * 58)
        print(time.strftime("LIVE INSURANCE DASHBOARD  %H:%M:%S"))
        print("=" * 58)
        print(f" Events processed : {int(g('processed')):>6}   rejected: {int(g('rejected'))}")
        print(f" Queue backlog    : {self.stream.qsize():>6}   avg latency: {g('avg_latency_ms'):.2f} ms")
        print(f" Policies         : {int(g('policies')):>6}   monthly premium: ${g('monthly_premium_total'):,.2f}")
        print(f" Payments         : {int(g('payments')):>6}   collected: ${g('payments_total'):,.2f}")
        print(f" Claims           : {int(g('claims')):>6}   total claimed: ${g('claims_amount_total'):,.2f}")
        print(f"   auto-approved  : {int(g('claims_auto_approved'))}")
        print(f"   manual review  : {int(g('claims_manual_review'))}")
        print(f"   flagged fraud  : {int(g('claims_flagged_fraud'))}")


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Real-time insurance data processing demo")
    parser.add_argument("--duration", type=int, default=30, help="seconds to run")
    parser.add_argument("--rate", type=float, default=10, help="events per second")
    parser.add_argument("--report-every", type=float, default=5, help="dashboard interval (s)")
    args = parser.parse_args()

    stream = queue.Queue()
    stats = Stats()
    stop = threading.Event()

    producer = EventProducer(stream, args.rate, stop)
    processor = EventProcessor(stream, stats)
    reporter = Reporter(stats, stream, args.report_every, stop)

    print(f"Starting pipeline: {args.rate} events/s for {args.duration}s ... (Ctrl+C to stop)")
    processor.start()
    producer.start()
    reporter.start()

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\nStopping ...")
    finally:
        stop.set()
        producer.join()
        stream.put(None)          # tell the processor to finish
        processor.join()
        reporter.print_report()
        print(f"\nDone. Data saved in '{DB_FILE}' (tables: policies, claims, payments, rejected).")
        print("Try:  sqlite3 insurance.db \"SELECT status, COUNT(*) FROM claims GROUP BY status;\"")


if __name__ == "__main__":
    main()
