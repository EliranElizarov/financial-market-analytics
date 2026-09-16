import os
import boto3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# הגדרות חיבור ל-MinIO בשרת המקומי (Docker)
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

def run_backfill():
    print("[BACKFILL] Starting 3-month historical data ingestion...")
    
    # טווח של 90 ימים אחורה מהיום
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    all_records = []

    for asset in ASSETS:
        print(f"[BACKFILL] Fetching historical data for {asset}...")
        ticker = yf.Ticker(asset)
        df = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d")
        
        for timestamp, row in df.iterrows():
            all_records.append({
                "asset": asset,
                "price": float(row["Close"]),
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
            })

    # יצירת DataFrame ושמירה כ-Parquet
    df_all = pd.DataFrame(all_records)
    local_filename = "historical_market_prices.parquet"
    df_all.to_parquet(local_filename)

    # העלאה ל-MinIO
    s3_key = "data/market_prices/historical_market_prices.parquet"
    s3_client.upload_file(local_filename, BUCKET_NAME, s3_key)
    print(f"[BACKFILL SUCCESS] Uploaded {len(df_all)} historical records to MinIO bucket '{BUCKET_NAME}' under '{s3_key}'!")

    # ניקוי קובץ מקומי זמני
    if os.path.exists(local_filename):
        os.remove(local_filename)

if __name__ == "__main__":
    run_backfill()