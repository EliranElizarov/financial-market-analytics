import os
import random
import boto3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

S3_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "admin1234"
BUCKET_NAME = "finalproject"

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

ASSETS = ["BTC-USD", "GC=F", "CL=F"]

def generate_realistic_news_backfill():
    print("[REALISTIC NEWS BACKFILL] Fetching price trends to derive consistent sentiment...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    news_records = []

    for asset in ASSETS:
        ticker = yf.Ticker(asset)
        df = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")
        
        # חישוב שינוי מחיר באחוזים מיום קודם (Pct Change)
        df["daily_return"] = df["Close"].pct_change().fillna(0)

        for timestamp, row in df.iterrows():
            pct_change = row["daily_return"]
            
            # גזירת סנטימנט ריאליסטי לפי מחירי השוק האמיתיים
            # אם המחיר עלה -> סנטימנט חיובי פרופורציונלי. אם ירד -> שלילי.
            base_sentiment = max(min(pct_change * 15, 0.95), -0.95)
            # הוספת רעש קל (Noise) לייצוג תנודתיות בשוק
            sentiment = round(base_sentiment + random.uniform(-0.1, 0.1), 2)
            sentiment = max(min(sentiment, 1.0), -1.0)

            news_records.append({
                "asset": asset,
                "title": f"Market report for {asset} - Daily trend analysis",
                "publisher": "Financial Market News",
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "sentiment_polarity": sentiment
            })

    df_news = pd.DataFrame(news_records)
    local_file = "historical_market_news.parquet"
    df_news.to_parquet(local_file)

    s3_key = "data/market_news/historical_market_news.parquet"
    s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
    print(f"[SUCCESS] Uploaded {len(df_news)} realistic news sentiment records to MinIO!")

    if os.path.exists(local_file):
        os.remove(local_file)

if __name__ == "__main__":
    generate_realistic_news_backfill()