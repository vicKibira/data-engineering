"""
producer.py — Pizza Sales Kafka Producer (Streaming Simulator)

Reads pizza_sales.csv and publishes each row as a JSON message
to a Kafka topic, simulating a real-time order stream.

Flow:
    pizza_sales.csv → producer.py → Kafka topic (pizza_sales) → Mage consumer

Usage:
    pip install kafka-python pandas
    python producer.py

Watch messages arrive live at: http://localhost:8080 (Redpanda Console)
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP = "localhost:9092"    # Kafka on host network
TOPIC     = "pizza_sales"       # Kafka topic name (created automatically on first send)
DELAY     = 0.2                 # seconds between rows → ~5 orders/sec
                                # set to 0.01 for fast replay, 1.0 to slow it down

# Producer setup
# value_serializer converts each Python dict → JSON bytes before sending
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Load CSV 
# pizza_sales.csv must be in the same folder as this script
df = pd.read_csv("pizza_sales.csv")
total = len(df)

print(f"Streaming {total} rows to topic '{TOPIC}' at ~{1/DELAY:.0f} rows/sec...")
print(f"Watch live at: http://localhost:8080\n")

# Stream rows 
# Each row becomes one Kafka message (one pizza order event)
for i, (_, row) in enumerate(df.iterrows(), 1):

    # Build the event payload — only the fields we care about
    record = {
        "pizza_id":       row["pizza_id"],
        "order_id":       row["order_id"],
        "pizza_name":     row["pizza_name"],
        "pizza_category": row["pizza_category"],
        "pizza_size":     row["pizza_size"],
        "quantity":       row["quantity"],
        "unit_price":     row["unit_price"],
        "total_price":    row["total_price"],
        "order_date":     row["order_date"],
        "order_time":     row["order_time"],
    }

    # Send to Kafka — non-blocking, batched internally by the producer
    producer.send(TOPIC, value=record)

    # Print a progress line every 100 rows so we know it's running
    if i % 100 == 0:
        print(f"  [{i}/{total}] order_id={int(row['order_id'])} | {row['pizza_name']} | ${row['total_price']:.2f}")

    # Simulate real-time delay between orders
    time.sleep(DELAY)

# ── Flush ─────────────────────────────────────────────────────
# Ensure all buffered messages are sent before the script exits
producer.flush()
print("\nDone — all rows sent to Kafka.")