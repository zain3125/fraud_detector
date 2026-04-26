import json
import psycopg2
from kafka import KafkaConsumer
import os
import time
from utils.logger import get_logger
from prometheus_client import start_http_server, Counter 

start_http_server(8001)
logger = get_logger("consumer")
db_errors = Counter(
    "db_errors_total",
    "DB insert failures"
)

fraud_saved = Counter(
    "fraud_saved_total",
    "Fraud alerts saved"
)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'fraud_db')
DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'admin')

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, 
                database=DB_NAME, 
                user=DB_USER, 
                password=DB_PASS,
                connect_timeout=5
            )
            logger.info("db_connected")
            return conn

        except Exception as e:
            logger.warning(
                "db_not_available_retrying",
                extra={"error": str(e), "retry_in": 2}
            )
            time.sleep(2)

def create_db_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            txn_count INT,
            max_amount DOUBLE PRECISION,
            high_value_flag INT,
            location_change_flag INT,
            high_frequency_flag INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

    logger.info("db_table_ready")

def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                'fraud_alerts',
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=5000
            )

            logger.info("kafka_consumer_connected")
            return consumer

        except Exception as e:
            logger.warning(
                "kafka_not_available_retrying",
                extra={"error": str(e), "retry_in": 2}
            )
            time.sleep(2)

def main():
    create_db_table()
    consumer = get_kafka_consumer()

    logger.info("consumer_started")

    conn = get_db_connection()
    cur = conn.cursor()

    for message in consumer:
        alert = message.value

        logger.info(
            "message_received",
            extra={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "user_id": alert.get("user_id")
            }
        )

        is_fraud = any([
            alert.get('high_value_flag') == 1,
            alert.get('location_change_flag') == 1,
            alert.get('high_frequency_flag') == 1
        ])

        if is_fraud:
            try:
                cur.execute("""
                    INSERT INTO alerts (user_id, window_start, window_end, txn_count, max_amount, 
                                       high_value_flag, location_change_flag, high_frequency_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    alert['user_id'], alert['window_start'], alert['window_end'],
                    alert['txn_count'], alert['max_amount'], alert['high_value_flag'],
                    alert['location_change_flag'], alert['high_frequency_flag']
                ))
                conn.commit()

                logger.info(
                    "fraud_alert_saved",
                    extra={
                        "user_id": alert.get("user_id"),
                        "offset": message.offset
                    }
                )

            except Exception:
                logger.exception(
                    "db_insert_failed",
                    extra={
                        "user_id": alert.get("user_id"),
                        "alert": alert
                    }
                )
                conn.rollback()
        else:
            pass

if __name__ == "__main__":
    main()
