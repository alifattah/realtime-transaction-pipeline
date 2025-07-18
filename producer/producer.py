from kafka import KafkaProducer
import json
import random
from faker import Faker
from datetime import datetime
import time

fake = Faker('fa_IR')

# Kafka config
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'transactions_raw'

PAYMENT_TYPES = ["کارت اعتباری", "اینترنت", "POS"]

# Producer Kafka با فرمت json
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False).encode('utf-8')
)

order_id_counter = 1

while True:
    # تولید داده تصادفی
    quantity = random.randint(1, 10)
    price = round(random.uniform(10000, 500000), 2)
    user_id = random.randint(1000, 1100)
    product_id = random.randint(1, 100)
    payment_type = random.choice(
        PAYMENT_TYPES + [None, 'Invalid'])  # عمداً گاهی نامعتبر/None

    total_amount = round(quantity * price, 2)
    transaction_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    transaction = {
        'order_id': order_id_counter,
        'user_id': user_id,
        'product_id': product_id,
        'quantity': quantity,
        'price': price,
        'total_amount': total_amount,
        'payment_type': payment_type,
        'transaction_time': transaction_time
    }

    # ارسال به Kafka
    producer.send(TOPIC_NAME, transaction)
    print(f"Sent: {transaction}")

    order_id_counter += 1
    time.sleep(1)  # one transactions per second

# producer.flush()  # برای مواقعی که حلقه تموم می‌شه
