from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, window, count, approx_count_distinct, 
                                   when, to_json, to_timestamp, struct, max as spark_max, lit)
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
from utils.logger import get_logger
import time
from prometheus_client import start_http_server, Counter

start_http_server(8002)
logger = get_logger("fraud_streaming")

batches_processed = Counter(
    "spark_batches_total",
    "Total batches processed"
)

fraud_detected = Counter(
    "fraud_detected_total",
    "Total fraud records"
)

KAFKA_BROKER = "kafka:9092"
SPARK_MASTER = "spark://spark-master:7077"

HIGH_AMOUNT_THRESHOLD = 3000
LOCATION_CHANGE_TIME_WINDOW = 300
HIGH_FREQUENCY_THRESHOLD = 5
FREQUENCY_TIME_WINDOW = 600

TRANSACTION_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", StringType()),
    StructField("merchant_category", StringType()),
])

def create_spark_session():
    return SparkSession.builder \
        .appName("FraudDetection") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
        .getOrCreate()

def read_from_kafka(spark):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)")

    parsed = df.select(from_json(col("value"), TRANSACTION_SCHEMA).alias("data"))
    return parsed

def detect_fraud(transactions):

    # 1. timestamp parsing
    transactions = transactions.withColumn(
        "timestamp_ts",
        to_timestamp(col("data.timestamp"))
    )

    # 2. watermark for late events
    transactions = transactions.withWatermark(
        "timestamp_ts",
        "15 minutes"
    )

    # 3. windowed aggregation per user
    agg = transactions.groupBy(
        col("data.user_id").alias("user_id"),
        window(col("timestamp_ts"), f"{FREQUENCY_TIME_WINDOW} seconds")
    ).agg(
        count("*").alias("txn_count"),
        approx_count_distinct("data.country").alias("distinct_countries"),
        spark_max("data.amount").alias("max_amount")
    )

    # 4. flags (same as before but kept for compatibility)
    agg = agg.withColumn(
        "high_value_flag",
        when(col("max_amount") > HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)
    ).withColumn(
        "location_change_flag",
        when(col("distinct_countries") > 1, 1).otherwise(0)
    ).withColumn(
        "high_frequency_flag",
        when(col("txn_count") > HIGH_FREQUENCY_THRESHOLD, 1).otherwise(0)
    )

    # 5. NEW: fraud score (improvement)
    agg = agg.withColumn(
        "fraud_score",
        (col("high_value_flag") * 2 +
         col("location_change_flag") * 2 +
         col("high_frequency_flag") * 1)
    ).withColumn(
        "fraud_probability",
        col("fraud_score") / lit(5.0)
    )

    # 6. output (UNCHANGED schema for Kafka/consumer compatibility)
    result = agg.select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("txn_count"),
        col("distinct_countries"),
        col("max_amount"),
        col("high_value_flag"),
        col("location_change_flag"),
        col("high_frequency_flag"),
        col("fraud_score"),
        col("fraud_probability")
    )

    # 7. smarter filter (still compatible)
    result = result.filter(
        col("fraud_score") >= 2
    )

    return result

def write_to_kafka(df):
    # Prepare the data to be sent to Kafka as JSON strings
    df.select(
        col("user_id").alias("key"),
        to_json(struct([col(c) for c in df.columns])).alias("value")
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", "fraud_alerts") \
        .save()

def main():
    spark = create_spark_session()

    # Reduce log noise
    spark.sparkContext.setLogLevel("ERROR")

    transactions = read_from_kafka(spark)

    # Convert timestamp early
    transactions = transactions.withColumn("timestamp_ts", to_timestamp(col("data.timestamp")))

    def show_batch(raw_df, batchId):
        start_time = time.time()
        
        batches_processed.inc()

        try:
            has_data = raw_df.limit(1).count() > 0
            if not has_data:
                return

            logger.info("batch_received", extra={"batch_id": batchId})

            result_df = detect_fraud(raw_df)
            
            fraud_count = result_df.count()

            if fraud_count > 0:
                fraud_detected.inc(fraud_count)
                write_to_kafka(result_df)

            logger.info(
                "fraud_detected",
                extra={
                    "batch_id": batchId,
                    "fraud_records": fraud_count
                }
            )

            logger.info(
                "batch_completed",
                extra={
                    "batch_id": batchId,
                    "duration_sec": round(time.time() - start_time, 3)
                }
            )

        except Exception:
            logger.exception(
                "batch_processing_failed",
                extra={"batch_id": batchId}
            )

    query = transactions.writeStream \
        .outputMode("append") \
        .foreachBatch(show_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
