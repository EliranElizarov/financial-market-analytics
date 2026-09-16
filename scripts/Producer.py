from datetime import datetime
import json
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd
import yfinance as yf

# Kafka Configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_DATA = "market_data_stream"
TOPIC_NEWS = "market_news_stream"


def create_kafka_topics_automatically():
    """Automatically create Kafka topics if they do not exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER, client_id="topic_creator"
        )
        topic_list = [
            NewTopic(name=TOPIC_DATA, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_NEWS, num_partitions=1, replication_factor=1),
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(
            f"[INIT] Successfully created topics: '{TOPIC_DATA}' and '{TOPIC_NEWS}'"
        )
    except TopicAlreadyExistsError:
        print("[INIT] Topics already exist. Ready to stream.")
    except Exception as e:
        print(f"[INIT] Topic creation check: {e}")


def initialize_producer():
    """Initialize Kafka Producer with retry logic"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("[INIT] Connected to Kafka Producer successfully.")
            return producer
        except Exception as e:
            print(f"[RETRY] Waiting for Kafka broker to be ready... ({e})")
            time.sleep(5)


def stream_market_prices(producer):
    """Fetch live commodity and crypto prices and send to market_data_stream"""
    tickers = ["BTC-USD", "CL=F", "GC=F"]
    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            price = t.fast_info.get("lastPrice") or t.fast_info.get(
                "previousClose"
            )

            if not price or pd.isna(price):
                df = yf.download(
                    ticker, period="1d", interval="1m", progress=False
                )
                if not df.empty and "Close" in df:
                    price = df["Close"].iloc[-1]
                    if isinstance(price, pd.Series):
                        price = price.iloc[0]

            if price and not pd.isna(price):
                payload = {
                    "asset": ticker,
                    "price": round(float(price), 2),
                    "timestamp": timestamp_str,
                }
                producer.send(TOPIC_DATA, value=payload)
                print(f"[PRICE -> KAFKA] {payload}")
            else:
                print(f"[WARNING] Could not fetch price for {ticker}")

        except Exception as e:
            print(f"[ERROR] Fetching price for {ticker} failed: {e}")


def stream_market_news(producer, ticker_symbol="BTC-USD"):
    """Fetch live news headlines and send to market_news_stream"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        news_list = ticker.news

        for item in news_list[:5]:
            content = item.get("content", {})
            title = content.get("title") or item.get("title", "")
            publisher = content.get("provider", {}).get(
                "displayName"
            ) or item.get("publisher", "Unknown")

            pub_time_epoch = content.get("pubDate") or item.get(
                "providerPublishTime"
            )
            if pub_time_epoch:
                if isinstance(pub_time_epoch, str):
                    pub_time_str = pub_time_epoch
                else:
                    pub_time_str = datetime.fromtimestamp(
                        pub_time_epoch
                    ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                pub_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if not title:
                continue

            # הוספת שדה asset לפיילוד החדשות
            payload = {
                "asset": ticker_symbol,
                "title": title,
                "publisher": publisher,
                "timestamp": pub_time_str,
            }
            producer.send(TOPIC_NEWS, value=payload)
            print(f"[NEWS -> KAFKA] {payload}")
    except Exception as e:
        print(f"[ERROR] Fetching news failed: {e}")


if __name__ == "__main__":
    create_kafka_topics_automatically()
    kafka_producer = initialize_producer()

    print(
        "\n=== Starting Live Streaming Pipeline: Yahoo Finance -> Kafka Topics ==="
    )

    while True:
        stream_market_prices(kafka_producer)
        stream_market_news(kafka_producer, "BTC-USD")
        kafka_producer.flush()
        print("--- Waiting 60 seconds for next cycle ---")
        time.sleep(60)