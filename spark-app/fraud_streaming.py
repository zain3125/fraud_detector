from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, approx_count_distinct, when, to_json, to_timestamp, struct, max as spark_max
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, TimestampType
import os

KAFKA_BROKER = "kafka:9092"
SPARK_MASTER = "spark://spark-master:7077"

HIGH_AMOUNT_THRESHOLD = 3000
LOCATION_CHANGE_TIME_WINDOW = 300
HIGH_FREQUENCY_THRESHOLD = 5
FREQUENCY_TIME_WINDOW = 600

TRANSACTION_SCHEMA = StructType([
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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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
    # convert timestamp and add watermark so aggregations can be cleaned up
    transactions = transactions.withColumn("timestamp_ts", to_timestamp(col("data.timestamp")))
    transactions = transactions.withWatermark("timestamp_ts", "15 minutes")

    # aggregate per user over a frequency window (this window will be used for counts)
    agg = transactions.groupBy(
        col("data.user_id").alias("user_id"),
        window(col("timestamp_ts"), f"{FREQUENCY_TIME_WINDOW} seconds")
    ).agg(
        count("*").alias("txn_count"),
        approx_count_distinct("data.country").alias("distinct_countries"),
        spark_max("data.amount").alias("max_amount")
    )

    # derive flags
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

    # select output columns
    result = agg.select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("txn_count"),
        col("distinct_countries"),
        col("max_amount"),
        col("high_value_flag"),
        col("location_change_flag"),
        col("high_frequency_flag")
    )
    result = result.filter(
            (col("high_value_flag") == 1) | 
            (col("location_change_flag") == 1) | 
            (col("high_frequency_flag") == 1)
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
        print(f"\n--- Raw Transactions Batch {batchId} ---")
        raw_df.select(
            "data.transaction_id", 
            "data.user_id", 
            "data.amount", 
            "data.country", 
            "timestamp_ts"
        ).show(truncate=False)

        # Compute fraud after aggregation
        result_df = detect_fraud(raw_df)
        print(f"\n--- Fraud Detection Batch {batchId} ---")
        result_df.show(truncate=False)
        
        # Write the results to Kafka topic
        if not result_df.isEmpty():
            write_to_kafka(result_df)

    query = transactions.writeStream \
        .outputMode("append") \
        .foreachBatch(show_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()