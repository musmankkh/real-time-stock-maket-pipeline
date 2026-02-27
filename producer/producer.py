import time
import json
import requests
from kafka import KafkaProducer

# ─── Config ─────────────────────────────────────────────────────
API_KEY  = "YOUR_FINNHUB_API_KEY"       # ← Add your key here
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS  = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
TOPIC    = "stock-quotes"               # ← Must match consumer
BROKER   = "host.docker.internal:29092"
INTERVAL = 10                           # seconds between fetches

# ─── Kafka Producer ─────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)
print(f"Producer connected to Kafka ✅ | Topic: {TOPIC}")

# ─── Fetch Quote Function ────────────────────────────────────────
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        data["symbol"]     = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None

# ─── Main Loop ───────────────────────────────────────────────────
try:
    while True:
        for symbol in SYMBOLS:
            data = fetch_quote(symbol)
            if data:
                producer.send(TOPIC, value=data)
                producer.flush()   # ✅ ensure delivery
                print(f"✅ Sent → {symbol} | price: {data.get('c')} | time: {data.get('fetched_at')}")
        print(f"Sleeping {INTERVAL}s...\n")
        time.sleep(INTERVAL)

# ─── Graceful Shutdown ───────────────────────────────────────────
except KeyboardInterrupt:
    print("\nShutdown signal received...")

finally:
    producer.flush()
    producer.close()
    print("Producer closed. 👋")