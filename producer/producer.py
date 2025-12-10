from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

fake = Faker()

def genrate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant": fake.company(),
        "country": fake.country_code(),
        "timestamp": fake.iso8601(),
        "merchant_category": random.choice(["retail", "food", "gas", "travel", "entertainment"]),
    }

def main():
    count = 0
    while True:
        transaction = genrate_transaction()
        producer.send("transactions", value=transaction)
        count +=1

        # print message every 10 transactions
        if count % 10 ==0:
            print(f"Sent {count} transactions")
        time.sleep(random.uniform(1, 2))

if __name__ == "__main__":
    main()