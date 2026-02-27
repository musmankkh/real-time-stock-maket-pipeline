import time
import json
import requests
from kafka import KafkaProducer

API_KEY = ""
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

producer = KafkaProducer(bootstrap_servers=["host.docker.internal:29092"],
                         value_serializer=lambda x: json.dumps(x).encode("utf-8"))


def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


while True:
    for symbol in SYMBOLS:
        data = fetch_quote(symbol)
        if data:
            producer.send("stock_quotes", value=data)
    time.sleep(10)