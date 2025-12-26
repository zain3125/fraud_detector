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

def create_db_table():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
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

def main():
    time.sleep(10)
    create_db_table()
    
    consumer = KafkaConsumer(
        'fraud_alerts',
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consumer is listening for fraud alerts...")

    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()

    for message in consumer:
        alert = message.value
        print(f"New Alert Received: {alert}")
        
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
        except Exception as e:
            print(f"Error inserting to DB: {e}")
            conn.rollback()

if __name__ == "__main__":
    main()