import json
import os
import sys
import urllib.request

os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin1234"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_REGION"] = "us-east-1"

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    lit,
    max,
    min,
    to_date,
)


def create_spark_session():
    conf = SparkConf()
    conf.set(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.540,"
        "org.postgresql:postgresql:42.6.0",
    )

    conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    conf.set("spark.hadoop.fs.s3a.access.key", "admin")
    conf.set("spark.hadoop.fs.s3a.secret.key", "admin1234")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    conf.set("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    conf.set("spark.hadoop.fs.s3a.change.detection.mode", "none")

    conf.set("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    conf.set("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "60000")
    conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")

    return (
        SparkSession.builder.appName("MarketBatchAnalytics")
        .config(conf=conf)
        .getOrCreate()
    )


def write_to_elasticsearch(summary_df):
    print(
        "[BATCH PROCESS] Indexing batch summary into Elasticsearch via HTTP API (http://elasticsearch:9200)..."
    )
    try:
        rows = summary_df.collect()

        # בניית פקודת Bulk בפורמט NDJSON המותאם ל-Elasticsearch
        bulk_data = ""
        for row in rows:
            action_line = json.dumps({"index": {"_index": "market-analytics"}})
            source_line = json.dumps({
                "execution_date": str(row["execution_date"]),
                "asset": row["asset"],
                "total_price_records": int(
                    row["total_price_records"] or 0
                ),
                "avg_price": float(row["avg_price"] or 0.0),
                "min_price": float(row["min_price"] or 0.0),
                "max_price": float(row["max_price"] or 0.0),
                "avg_sentiment": float(row["avg_sentiment"] or 0.0),
                "total_news_records": int(row["total_news_records"] or 0),
                "created_at": str(row["created_at"]),
            })
            bulk_data += action_line + "\n" + source_line + "\n"

        req = urllib.request.Request(
            "http://elasticsearch:9200/_bulk",
            data=bulk_data.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            method="POST",
        )

        with urllib.request.urlopen(req) as response:
            res_body = response.read().decode("utf-8")
            print(
                f"[BATCH PROCESS SUCCESS] Successfully indexed data into Elasticsearch! Response summary received."
            )

    except Exception as e:
        print(f"[BATCH PROCESS ERROR] Failed writing to Elasticsearch: {e}")


def run_daily_correlation_batch():
    print("[BATCH PROCESS] Starting Spark batch correlation analysis...")
    spark = create_spark_session()

    hist_prices_path = "s3a://finalproject/data/market_prices/"
    stream_prices_path = "s3a://finalproject/data/raw_stream_prices/"

    hist_news_path = "s3a://finalproject/data/market_news/"
    stream_news_path = "s3a://finalproject/data/raw_stream_news/"

    output_path = "s3a://finalproject/data/batch_summary/"

    try:
        print(
            "[BATCH PROCESS] Reading market prices from historical and stream paths..."
        )
        df_prices = spark.read.parquet(hist_prices_path)

        try:
            df_stream_prices = spark.read.parquet(stream_prices_path)
            df_prices = df_prices.unionByName(
                df_stream_prices, allowMissingColumns=True
            )
        except Exception:
            print("[BATCH INFO] Using historical price data only.")

        if df_prices.rdd.isEmpty():
            print("[BATCH PROCESS WARN] No market price data found to process.")
            spark.stop()
            return

        # 1. אגרגציית מחירי שוק לפי תאריך ונכס
        df_prices_agg = (
            df_prices.withColumn("execution_date", to_date(col("timestamp")))
            .groupBy("execution_date", "asset")
            .agg(
                count("price").alias("total_price_records"),
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
            )
        )

        # 2. קריאת חדשות ואגרגציית סנטימנט
        has_news_sentiment = False
        try:
            print(
                "[BATCH PROCESS] Reading market news from historical and stream paths..."
            )
            df_news = spark.read.parquet(hist_news_path)

            try:
                df_stream_news = spark.read.parquet(stream_news_path)
                df_news = df_news.unionByName(
                    df_stream_news, allowMissingColumns=True
                )
            except Exception:
                pass

            if "asset" in df_news.columns and (
                "sentiment_polarity" in df_news.columns
                or "sentiment_score" in df_news.columns
            ):
                sent_col = (
                    "sentiment_polarity"
                    if "sentiment_polarity" in df_news.columns
                    else "sentiment_score"
                )

                df_news_agg = (
                    df_news.withColumn(
                        "execution_date", to_date(col("timestamp"))
                    )
                    .groupBy("execution_date", "asset")
                    .agg(
                        avg(col(sent_col)).alias("avg_sentiment"),
                        count(col(sent_col)).alias("total_news_records"),
                    )
                )

                summary_df = df_prices_agg.join(
                    df_news_agg, on=["execution_date", "asset"], how="left"
                )
                has_news_sentiment = True

        except Exception as news_err:
            print(f"[BATCH PROCESS WARN] Could not join news data: {news_err}")

        if not has_news_sentiment:
            summary_df = df_prices_agg.withColumn(
                "avg_sentiment", lit(0.0)
            ).withColumn("total_news_records", lit(0))

        summary_df = summary_df.withColumn("created_at", current_timestamp())
        summary_df.show(truncate=False)

        # 3. כתיבה ל-MinIO
        print(f"[BATCH PROCESS] Writing batch summary to MinIO: {output_path}")
        summary_df.write.mode("overwrite").parquet(output_path)

        # 4. כתיבה ל-PostgreSQL
        print("[BATCH PROCESS] Writing batch summary to PostgreSQL...")
        postgres_url = "jdbc:postgresql://postgres:5432/postgres"
        postgres_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver",
        }

        summary_df.write.jdbc(
            url=postgres_url,
            table="market_batch_summary",
            mode="overwrite",
            properties=postgres_properties,
        )
        print("[BATCH PROCESS SUCCESS] Data loaded successfully to PostgreSQL!")

        # 5. כתיבה ל-Elasticsearch דרך ה-HTTP API הישיר
        write_to_elasticsearch(summary_df)

    except Exception as e:
        print(f"[BATCH PROCESS ERROR] Failed during Spark batch execution: {e}")
        raise e
    finally:
        spark.stop()
        print("[BATCH PROCESS] Spark session stopped successfully.")


if __name__ == "__main__":
    run_daily_correlation_batch()