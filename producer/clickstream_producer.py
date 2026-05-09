"""
E-Commerce Clickstream Producer
--------------------------------
Generates synthetic user events for an online electronics store and
publishes them to a Kafka topic.

Event schema:
    {
        "user_id":     str,
        "product_id":  str,
        "category":    str,
        "event_type":  "view" | "add_to_cart" | "purchase",
        "timestamp":   ISO-8601 string (event time)
    }

The generator is intentionally biased so that, occasionally, a single
product receives a burst of "view" events with very few "purchase"
events. This is what triggers the real-time "Flash Sale" alert in the
Spark streaming job (>100 views, <5 purchases inside a 10-minute
sliding window).
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

#  config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "20"))


CATALOGUE = [
    {"product_id": "P001", "category": "Laptops"},
    {"product_id": "P002", "category": "Laptops"},
    {"product_id": "P003", "category": "Smartphones"},
    {"product_id": "P004", "category": "Smartphones"},
    {"product_id": "P005", "category": "Smartphones"},
    {"product_id": "P006", "category": "Headphones"},
    {"product_id": "P007", "category": "Headphones"},
    {"product_id": "P008", "category": "Cameras"},
    {"product_id": "P009", "category": "Cameras"},
    {"product_id": "P010", "category": "Smartwatches"},
    {"product_id": "P011", "category": "Smartwatches"},
    {"product_id": "P012", "category": "Tablets"},
]

USERS = [f"U{str(i).zfill(4)}" for i in range(1, 201)]  

# Probability mass for normal traffic: views >> add_to_cart >> purchase
NORMAL_EVENT_WEIGHTS = {
    "view": 0.80,
    "add_to_cart": 0.15,
    "purchase": 0.05,
}


def build_producer() -> KafkaProducer:
    """Retry-loop until Kafka is reachable."""
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt + 1}/30) - retrying in 3s")
            time.sleep(3)
    print("[producer] FATAL: Kafka never became available", file=sys.stderr)
    sys.exit(1)


def pick_event_type(weights: dict[str, float]) -> str:
    r = random.random()
    cumulative = 0.0
    for event, weight in weights.items():
        cumulative += weight
        if r <= cumulative:
            return event
    return "view"


def make_event(product: dict, user: str, event_type: str) -> dict:
    return {
        "user_id": user,
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# -------------------------------------------------------------------- main loop
def main() -> None:
    producer = build_producer()
    print(f"[producer] Connected to Kafka at {KAFKA_BOOTSTRAP}")
    print(f"[producer] Producing {EVENTS_PER_SECOND} events/sec to topic '{TOPIC}'")

    # Graceful shutdown
    stop = {"flag": False}

    def _shutdown(*_):
        stop["flag"] = True
        print("\n[producer] Shutdown signal received - flushing...")

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    sleep_per_event = 1.0 / max(EVENTS_PER_SECOND, 1)
    sent = 0
    flash_sale_burst_remaining = 0
    burst_product: dict | None = None

    while not stop["flag"]:
        
        if flash_sale_burst_remaining == 0 and random.random() < (1 / 1500):
            burst_product = random.choice(CATALOGUE)
            flash_sale_burst_remaining = random.randint(150, 220)
            print(
                f"[producer] >>> Injecting flash-sale burst on "
                f"{burst_product['product_id']} ({burst_product['category']}) "
                f"for {flash_sale_burst_remaining} events"
            )

        if flash_sale_burst_remaining > 0 and burst_product is not None:
            event = make_event(burst_product, random.choice(USERS), "view")
            flash_sale_burst_remaining -= 1
        else:
            product = random.choice(CATALOGUE)
            event_type = pick_event_type(NORMAL_EVENT_WEIGHTS)
            event = make_event(product, random.choice(USERS), event_type)

        # Partition by user_id so a single user's events stay ordered
        producer.send(TOPIC, key=event["user_id"], value=event)
        sent += 1

        if sent % 100 == 0:
            print(f"[producer] sent {sent} events (latest: {event['event_type']} "
                  f"on {event['product_id']})")

        time.sleep(sleep_per_event)

    producer.flush()
    producer.close()
    print(f"[producer] Done. Total sent: {sent}")


if __name__ == "__main__":
    main()
