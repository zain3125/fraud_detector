import json
import psycopg2
from kafka import KafkaConsumer
import os
import time

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
            return conn
        except Exception as e:
            print(f"Waiting for Postgres... ({e})")
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
    print("Database table is ready.")

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
            return consumer
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(2)

def main():
    create_db_table()    
    consumer = get_kafka_consumer()
    print("Consumer is listening for fraud alerts...")

    conn = get_db_connection()
    cur = conn.cursor()

    for message in consumer:
        alert = message.value
        
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
                print(f"Fraud Alert Saved for User: {alert['user_id']}")
            except Exception as e:
                print(f"Error inserting to DB: {e}")
                conn.rollback()
        else:
            pass

if __name__ == "__main__":
    main()