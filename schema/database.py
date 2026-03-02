import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

# ─── Snowflake Connection ────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()

# ─── Create Table ────────────────────────────────────────────────
create_table_sql = """
CREATE TABLE IF NOT EXISTS RAWSTOCKS (
    id              NUMBER AUTOINCREMENT PRIMARY KEY,
    symbol          VARCHAR(20)     NOT NULL,
    fetched_at      VARCHAR(20)     NOT NULL,
    open_price      FLOAT,
    high_price      FLOAT,
    low_price       FLOAT,
    current_price   FLOAT,
    prev_close      FLOAT,
    raw_record      VARIANT,
    inserted_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
"""

try:
    cursor.execute(create_table_sql)
    print("✅ Table RAWSTOCKS created successfully.")
except Exception as e:
    print(f"❌ Failed to create table: {e}")
finally:
    cursor.close()
    conn.close()
    print("Connection closed. 👋")