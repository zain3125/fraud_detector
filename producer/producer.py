from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from utils.logger import get_logger
import json
import random
import time
from prometheus_client import start_http_server, Counter

messages_sent = Counter(
    "messages_sent_total",
    "Total messages sent"
)
start_http_server(8000)
logger = get_logger("producer")
messages_sent.inc()

def create_producer():
    while True:
        try:
            logger.info("kafka_connection_attempt")
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("kafka_connected")
            return producer

        except NoBrokersAvailable:
            logger.warning("kafka_not_available_retrying", extra={"retry_in": 3})
            time.sleep(3)

producer = create_producer()

fake = Faker()
logger.info("producer_started")

def generate_transaction():
    is_fraud = random.random() < 0.10

    user_id = fake.uuid4()

    base_country = random.choice(["US", "UK", "DE", "FR"])

    # normal behavior
    amount = random.uniform(10, 3000)
    country = base_country
    merchant_category = random.choice(["retail", "food", "gas"])

    if is_fraud:
        # subtle fraud pattern
        amount = random.uniform(2000, 8000)
        country = random.choice(["NG", "BR", "RU", "CN"])
        merchant_category = random.choice(["electronics", "luxury", "travel"])

    return {
        "event_id": fake.uuid4(),
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant": fake.company(),
        "country": country,
        "timestamp": fake.iso8601(),
        "merchant_category": merchant_category,
    }

def main():
    count = 0

    while True:
        batch_size = random.randint(1, 15) 
        
        for _ in range(batch_size):
            transaction = generate_transaction()
            producer.send("transactions", value=transaction)
            messages_sent.inc()
            count += 1

        if count % 10 == 0:
            logger.info(
                "transactions_sent",
                extra={
                    "count": count,
                    "last_event_id": transaction["event_id"]
                }
            )

        time.sleep(random.uniform(0.5, 3.0))

if __name__ == "__main__":
    main()
