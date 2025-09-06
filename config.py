import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TICKS_TOPIC = os.getenv("TICKS_TOPIC", "ticks")
NEWS_TOPIC  = os.getenv("NEWS_TOPIC", "news")

SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA").split(",")

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "REPLACE_ME")
NEWS_QUERY   = os.getenv("NEWS_QUERY", "stocks OR market OR earnings")
NEWS_LANG    = os.getenv("NEWS_LANG", "en")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

MC_PATHS     = int(os.getenv("MC_PATHS", "100000"))
MC_HORIZON_D = int(os.getenv("MC_HORIZON_D", "30"))
RISK_FREE    = float(os.getenv("RISK_FREE", "0.02"))

ROLLING_WINDOW_SEC = int(os.getenv("ROLLING_WINDOW_SEC", "900"))
WATERMARK_SEC       = int(os.getenv("WATERMARK_SEC", "120"))
