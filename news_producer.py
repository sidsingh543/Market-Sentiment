import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import requests

from config import (
    KAFKA_BOOTSTRAP, NEWS_TOPIC,
    NEWS_API_KEY, NEWS_QUERY, NEWS_LANG
)

API_URL = "https://newsapi.org/v2/everything"

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        retries=10,
        acks="all",
    )

def fetch_news():
    params = {
        "q": NEWS_QUERY,
        "language": NEWS_LANG,
        "sortBy": "publishedAt",
        "pageSize": 20,
        "apiKey": NEWS_API_KEY,
    }
    r = requests.get(API_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("articles", [])

def main():
    producer = make_producer()
    seen_urls = set()
    print(f"[news_producer] sending headlines to topic '{NEWS_TOPIC}'")

    while True:
        try:
            articles = fetch_news()
            for a in articles:
                url = a.get("url")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                payload = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "title": a.get("title"),
                    "description": a.get("description"),
                    "source": (a.get("source") or {}).get("name"),
                    "url": url,
                }
                producer.send(NEWS_TOPIC, value=payload)
            producer.flush()
        except Exception as e:
            print("[news_producer] error:", e)

        time.sleep(30)

if __name__ == "__main__":
    main()
