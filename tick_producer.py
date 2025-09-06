import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf
import numpy as np

from config import KAFKA_BOOTSTRAP, TICKS_TOPIC, SYMBOLS

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=10,
        acks="all",
    )

def fetch_price(symbol):
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="1d", interval="1m")
        if df.empty:
            return None
        price = float(df["Close"].iloc[-1])
        return price
    except Exception:
        return None

def gbm_step(s, mu=0.02, sigma=0.25, dt=1/390):
    z = np.random.normal()
    return s * np.exp((mu - 0.5*sigma**2)*dt + sigma*np.sqrt(dt)*z)

def main():
    producer = make_producer()
    last_prices = {}

    print(f"[tick_producer] sending ticks for {SYMBOLS} to topic '{TICKS_TOPIC}'")

    while True:
        for symbol in SYMBOLS:
            now = datetime.now(timezone.utc).isoformat()
            price = fetch_price(symbol)

            if price is None:
                base = last_prices.get(symbol, 100.0 + random.random()*50)
                price = gbm_step(base)
            last_prices[symbol] = price

            payload = {
                "ts": now,
                "symbol": symbol,
                "price": round(float(price), 6),
            }
            producer.send(TICKS_TOPIC, value=payload)

        producer.flush()
        time.sleep(5)

if __name__ == "__main__":
    main()
