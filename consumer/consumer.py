# ─── Imports ────────────────────────────────────────────────────
import json
import time
from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()  # Load Snowflake credentials from .env file

# ─── Kafka Config ───────────────────────────────────────────────
KAFKA_BROKER = "host.docker.internal:29092"
KAFKA_TOPIC  = "stock-quotes"          # ← Must match producer
KAFKA_GROUP  = "bronze-consumer1"

# ─── Snowflake Config ───────────────────────────────────────────
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB        = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE     = "RAWSTOCKS"

# ─── Batch Size ─────────────────────────────────────────────────
BATCH_SIZE = 100

# ─── Snowflake Connection ────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DB,
    schema=SNOWFLAKE_SCHEMA
)
cursor = conn.cursor()
print("Snowflake connected ✅")



# ─── Kafka Consumer ──────────────────────────────────────────────
print("Connecting to Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)
print(f"Listening to topic '{KAFKA_TOPIC}' ✅")

# ─── Insert Batch Function ───────────────────────────────────────
def insert_batch(batch):
    try:
        cursor.executemany(
            f"""
            INSERT INTO {SNOWFLAKE_TABLE} 
                (symbol, fetched_at, open_price, high_price, low_price, current_price, prev_close, raw_record)
            SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
            """,
            batch
        )
        conn.commit()
        print(f"✅ Inserted batch of {len(batch)} records into Snowflake.")
    except Exception as e:
        print(f"❌ Batch insert failed: {e}")
        conn.rollback()

# ─── Main Loop ───────────────────────────────────────────────────
batch = []

try:
    for message in consumer:
        record = message.value

        # Extract Finnhub fields
        symbol        = record.get("symbol", "unknown")
        fetched_at    = str(record.get("fetched_at", int(time.time())))
        open_price    = record.get("o")    # open
        high_price    = record.get("h")    # high
        low_price     = record.get("l")    # low
        current_price = record.get("c")    # current
        prev_close    = record.get("pc")   # previous close

        batch.append((
            symbol,
            fetched_at,
            open_price,
            high_price,
            low_price,
            current_price,
            prev_close,
            json.dumps(record)
        ))

        print(f"📨 Received → {symbol} | Price: {current_price} | Time: {fetched_at}")

        # ── Insert when batch is full ──
        if len(batch) >= BATCH_SIZE:
            insert_batch(batch)
            batch.clear()

# ─── Graceful Shutdown ───────────────────────────────────────────
except KeyboardInterrupt:
    print("\nShutdown signal received (Ctrl+C)...")

finally:
    # Insert remaining records
    if batch:
        print(f"Inserting remaining {len(batch)} records...")
        insert_batch(batch)

    cursor.close()
    conn.close()
    consumer.close()
    print("All connections closed. Consumer stopped. 👋")