"""
consumer.py — Kafka → PostgreSQL Consumer

Reads pizza order events from the 'pizza_sales' Kafka topic
and inserts them into a PostgreSQL table in real-time.

Flow:
    Kafka topic (pizza_sales)
        → consumer.py polls messages
            → batches 50 rows
                → bulk inserts into PostgreSQL
                    → commits Kafka offset (marks messages as processed)

Why batch instead of row-by-row?
    A DB commit per message would be ~48,000 individual transactions.
    Batching 50 at a time reduces that to ~960 commits — much faster.

Why manual offset commit (enable_auto_commit=False)?
    Auto-commit would mark messages as processed BEFORE we write to the DB.
    If the script crashes mid-batch, those messages would be lost.
    Manual commit means: only mark processed AFTER the DB insert succeeds.
    This gives us at-least-once delivery (safe) instead of at-most-once (lossy).

Usage:
    pip install kafka-python-ng psycopg2-binary
    python consumer.py
"""

import json
import psycopg2
from kafka import KafkaConsumer

# ── Kafka config ──────────────────────────────────────────────
BOOTSTRAP = "localhost:9092"
TOPIC     = "pizza_sales"
GROUP_ID  = "pizza-pg-consumer"
# GROUP_ID is how Kafka tracks which messages THIS consumer has already seen.
# If you restart consumer.py, it picks up from the last committed offset,
# not from the beginning — so no duplicate inserts.

# ── Postgres config ───────────────────────────────────────────
DB = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "postgres",
}

# ── DDL — table definition ────────────────────────────────────
# Runs once at startup. IF NOT EXISTS means it's safe to re-run —
# if the table already exists it does nothing.
# inserted_at is added automatically by Postgres so we can see
# exactly when each order was consumed from the stream.
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pizza_sales (
    id              SERIAL PRIMARY KEY,
    pizza_id        NUMERIC,
    order_id        NUMERIC,
    pizza_name      TEXT,
    pizza_category  TEXT,
    pizza_size      TEXT,
    quantity        NUMERIC,
    unit_price      NUMERIC,
    total_price     NUMERIC,
    order_date      TEXT,
    order_time      TEXT,
    inserted_at     TIMESTAMPTZ DEFAULT NOW()   -- set automatically on insert
);
"""

# ── Insert query ──────────────────────────────────────────────
# %(key)s syntax is psycopg2's named parameter format.
# It maps directly to the dict keys in each Kafka message,
# so we never have to manually unpack fields.
INSERT_ROW = """
INSERT INTO pizza_sales (
    pizza_id, order_id, pizza_name, pizza_category,
    pizza_size, quantity, unit_price, total_price,
    order_date, order_time
) VALUES (
    %(pizza_id)s, %(order_id)s, %(pizza_name)s, %(pizza_category)s,
    %(pizza_size)s, %(quantity)s, %(unit_price)s, %(total_price)s,
    %(order_date)s, %(order_time)s
);
"""

# ── Connect to Postgres ───────────────────────────────────────
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**DB)
conn.autocommit = False   # we control when transactions commit (for batching)
cur = conn.cursor()

# Create table if it doesn't already exist
cur.execute(CREATE_TABLE)
conn.commit()
print("Table ready.")

# ── Connect to Kafka ──────────────────────────────────────────
print(f"Connecting to Kafka topic '{TOPIC}'...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,

    # "earliest" = start from message 0 if this group has never consumed before.
    # Once offsets are committed, this setting is ignored and it resumes from
    # the last committed offset instead.
    auto_offset_reset="earliest",

    # Disable auto-commit so we only mark messages as processed
    # AFTER they are safely written to the database (see module docstring).
    enable_auto_commit=False,

    # Decode incoming bytes → JSON dict automatically
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print("Listening for messages...\n")

# ── Consume + batch insert loop ───────────────────────────────
BATCH_SIZE = 50   # how many rows to accumulate before writing to DB
batch = []

try:
    for msg in consumer:
        # msg.value is already a dict thanks to value_deserializer above
        batch.append(msg.value)

        # Once the batch is full, flush it to Postgres
        if len(batch) >= BATCH_SIZE:

            # executemany sends all rows in one round-trip to the DB
            cur.executemany(INSERT_ROW, batch)

            # Commit the DB transaction — rows are now persisted
            conn.commit()

            # Commit the Kafka offset — broker knows we processed these messages
            # This MUST happen after conn.commit(), not before
            consumer.commit()

            print(f"  Inserted {len(batch)} rows | "
                  f"last order_id={int(batch[-1]['order_id'])} | "
                  f"{batch[-1]['pizza_name']}")

            batch.clear()   # reset for the next batch

except KeyboardInterrupt:
    # Graceful shutdown on Ctrl+C:
    # flush whatever is left in the batch so we don't lose partial progress
    if batch:
        cur.executemany(INSERT_ROW, batch)
        conn.commit()
        consumer.commit()
        print(f"  Flushed final {len(batch)} rows on exit.")

    print("\nStopped cleanly.")

finally:
    # Always close connections even if an unexpected error occurs
    cur.close()
    conn.close()
    consumer.close()