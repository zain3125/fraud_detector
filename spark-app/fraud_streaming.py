from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when, to_json, struct
from pyspark.sql.types import StringType, StructType, StructField,  DoubleType, TimestampType
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
    return SparkSession.builder\
    .appName("FraudDetection")\
    .master("SPARK_MASTER")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-1-_2.12:3.5.0")\
    .getOrCreate()
