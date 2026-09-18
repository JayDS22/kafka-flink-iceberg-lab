"""
Synthetic click event producer.

Usage:
    pip install confluent-kafka
    python bench/producer.py --rate 1000 --duration 300
"""
from __future__ import annotations

import argparse
import json
import random
import string
import time

from confluent_kafka import Producer


def rand_user(n: int = 100) -> str:
    return f"u{random.randint(0, n - 1):04d}"


def event(now_ms: int, late_prob: float = 0.02) -> dict:
    # ~2% of events arrive 30-90s late to exercise watermark handling.
    ts = now_ms - random.randint(30_000, 90_000) if random.random() < late_prob else now_ms
    return {
        "user_id": rand_user(),
        "value": round(random.uniform(0.1, 100.0), 2),
        "ts_ms": ts,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="clicks")
    ap.add_argument("--rate", type=int, default=1000, help="events per second")
    ap.add_argument("--duration", type=int, default=60, help="seconds")
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5, "acks": "all"})

    interval = 1.0 / args.rate
    end = time.time() + args.duration
    sent = 0
    started = time.time()

    while time.time() < end:
        now_ms = int(time.time() * 1000)
        ev = event(now_ms)
        p.produce(args.topic, json.dumps(ev).encode())
        sent += 1
        if sent % args.rate == 0:
            p.poll(0)
        time.sleep(interval)

    p.flush(10)
    elapsed = time.time() - started
    print(f"sent={sent} elapsed={elapsed:.1f}s effective_rate={sent / elapsed:.0f}/s")


if __name__ == "__main__":
    main()


def _selfcheck() -> None:
    # ponytail: one runnable check per the skill's testing rule.
    # `python -c 'from bench.producer import _selfcheck; _selfcheck()'`
    ev = event(1_735_000_000_000, late_prob=0.0)
    assert set(ev) == {"user_id", "value", "ts_ms"}
    assert ev["ts_ms"] == 1_735_000_000_000
    assert ev["user_id"].startswith("u")
    late = event(1_735_000_000_000, late_prob=1.0)
    assert late["ts_ms"] < 1_735_000_000_000
    print("OK")
