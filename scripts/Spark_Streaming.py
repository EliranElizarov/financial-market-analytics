import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from textblob import TextBlob

os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin1234"


@F.udf(returnType=T.FloatType())
def get_sentiment_polarity(text):
    if text:
        return float(TextBlob(text).sentiment.polarity)
    return 0.0


spark = (
    SparkSession.builder.master("local[*]")
    .appName("CryptoPulse_Streaming_Pipeline")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin1234")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

data_schema = T.StructType(
    [
        T.StructField("asset", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("timestamp", T.StringType()),
    ]
)

news_schema = T.StructType(
    [
        T.StructField("asset", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("publisher", T.StringType()),
        T.StructField("timestamp", T.StringType()),
    ]
)

raw_data_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "course-kafka:9092")
    .option("subscribe", "market_data_stream")
    .option("startingOffsets", "earliest")
    .load()
    .select(F.col("value").cast(T.StringType()))
)

raw_news_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "course-kafka:9092")
    .option("subscribe", "market_news_stream")
    .option("startingOffsets", "earliest")
    .load()
    .select(F.col("value").cast(T.StringType()))
)

parsed_data = raw_data_stream.withColumn(
    "data", F.from_json(F.col("value"), data_schema)
).select("data.*").withColumn("event_time", F.to_timestamp(F.col("timestamp")))

parsed_news = (
    raw_news_stream.withColumn("data", F.from_json(F.col("value"), news_schema))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    .withColumn("sentiment_polarity", get_sentiment_polarity(F.col("title")))
)

# כתיבה לתיקיות נפרדות עבור הסטרימינג (מונע התנגשות מטא-דאטה מול ההיסטוריה)
query_data = (
    parsed_data.writeStream.format("parquet")
    .option("path", "s3a://finalproject/data/raw_stream_prices/")
    .option(
        "checkpointLocation", "s3a://finalproject/checkpoints/market_prices/"
    )
    .outputMode("append")
    .start()
)

query_news = (
    parsed_news.writeStream.format("parquet")
    .option("path", "s3a://finalproject/data/raw_stream_news/")
    .option(
        "checkpointLocation", "s3a://finalproject/checkpoints/market_news/"
    )
    .outputMode("append")
    .start()
)

print(
    "=== Spark Streaming Pipeline running: Consuming Kafka -> Writing to raw_stream paths ==="
)

spark.streams.awaitAnyTermination()