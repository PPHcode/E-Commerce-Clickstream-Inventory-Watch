"""
Spark Structured Streaming - Speed Layer
-----------------------------------------
Reads clickstream events from Kafka and runs two parallel pipelines:

1. RAW ARCHIVE
   Persists every event to Parquet partitioned by date. The Airflow
   batch layer reads this Parquet store once a day to build the
   segmentation report and the conversion-rate analytics.

2. FLASH-SALE DETECTOR
   Aggregates events in a 10-minute SLIDING window (5-minute slide)
   keyed by product_id. If the window has > 100 views AND < 5
   purchases, emit a "FLASH_SALE_SUGGESTED" alert to the
   `flash-sale-alerts` Kafka topic and to the console.

Event-time vs processing-time
-----------------------------
The event "timestamp" field carried by the producer is treated as
EVENT TIME. We attach a 2-minute watermark so late-arriving events
within that grace period are still folded into the right window;
later events are dropped by Spark.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC = "clickstream-events"
ALERT_TOPIC = "flash-sale-alerts"

RAW_PARQUET_PATH = "/opt/data/raw_events"
RAW_CHECKPOINT = "/opt/data/_checkpoints/raw"
ALERT_CHECKPOINT = "/opt/data/_checkpoints/alerts"
CONSOLE_CHECKPOINT = "/opt/data/_checkpoints/console"

EVENT_SCHEMA = StructType([
    StructField("user_id",    StringType(),    True),
    StructField("product_id", StringType(),    True),
    StructField("category",   StringType(),    True),
    StructField("event_type", StringType(),    True),
    StructField("timestamp",  TimestampType(), True),
])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("ClickstreamStreamingJob")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
           .select(F.from_json("json_str", EVENT_SCHEMA).alias("e"))
           .select("e.*")
           .withColumn("event_date", F.to_date("timestamp"))
    )

    #  Pipeline 1: raw archive
    raw_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", RAW_PARQUET_PATH)
        .option("checkpointLocation", RAW_CHECKPOINT)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Pipeline 2: flash-sale detector
    # 10-minute window sliding every 5 minutes, with a 2-minute watermark.
    windowed_counts = (
        parsed
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            F.window("timestamp", "10 minutes", "5 minutes"),
            F.col("product_id"),
            F.col("category"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
        )
    )

    alerts = (
        windowed_counts
        .filter((F.col("views") > 100) & (F.col("purchases") < 5))
        .select(
            F.col("product_id"),
            F.col("category"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("views"),
            F.col("purchases"),
            F.col("cart_adds"),
            F.lit("FLASH_SALE_SUGGESTED").alias("alert_type"),
            F.lit("High interest, low conversion - consider a discount or flash sale.")
                .alias("recommendation"),
        )
    )

    # Console sink: visible alert in the Spark container logs
    console_query = (
        alerts.writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", CONSOLE_CHECKPOINT)
        .outputMode("update")
        .start()
    )

    # Kafka sink: alert payload as JSON to flash-sale-alerts topic
    alerts_kafka = alerts.select(
        F.col("product_id").alias("key"),
        F.to_json(F.struct(*alerts.columns)).alias("value"),
    )

    alert_query = (
        alerts_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERT_TOPIC)
        .option("checkpointLocation", ALERT_CHECKPOINT)
        .outputMode("update")
        .start()
    )

    print("[spark] Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
