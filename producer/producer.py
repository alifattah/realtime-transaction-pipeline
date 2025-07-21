from kafka import KafkaProducer
import json
import random
from faker import Faker
from datetime import datetime
import time

# Initialize Faker to generate Persian-localised data
fake = Faker('fa_IR')

# --- Kafka Configuration ---
# Define the Kafka broker address and the target topic name.
KAFKA_BROKER = '127.0.0.1:9092'
TOPIC_NAME = 'transactions_raw'

# --- Data Generation Constants ---
# Define possible payment types for the transactions.
PAYMENT_TYPES = ["کارت اعتباری", "اینترنت", "POS"]

# --- Kafka Producer Initialization ---
# Create a KafkaProducer instance.
# - bootstrap_servers: Specifies the Kafka broker(s) to connect to.
# - value_serializer: A function to serialize the message value before sending.
#   Here, it converts the Python dictionary to a JSON string and encodes it to UTF-8.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False).encode('utf-8')
)

# Initialize a counter for generating unique order IDs.
order_id_counter = 1

# --- Main Loop for Data Production ---
# This loop runs indefinitely to continuously generate and send transaction data.
while True:
    # --- Generate Random Transaction Data ---
    # Create realistic-looking but random values for each transaction field.
    quantity = random.randint(1, 10)
    price = round(random.uniform(10000, 500000), 2)
    user_id = random.randint(1000, 1100)
    product_id = random.randint(1, 100)
    # Intentionally include None or 'Invalid' values sometimes for data quality testing.
    payment_type = random.choice(
        PAYMENT_TYPES + [None, 'Invalid'])

    # Calculate dependent fields.
    total_amount = round(quantity * price, 2)
    transaction_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- Assemble the Transaction Record ---
    # Structure the generated data into a dictionary.
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

    # --- Send Data to Kafka ---
    # Send the transaction dictionary to the specified Kafka topic.
    # The producer handles the serialization automatically based on the 'value_serializer' setup.
    producer.send(TOPIC_NAME, transaction)
    print(f"Sent: {transaction}")

    # Increment the order ID for the next message.
    order_id_counter += 1
    # Pause execution briefly to avoid overwhelming the system.
    time.sleep(0.1)
