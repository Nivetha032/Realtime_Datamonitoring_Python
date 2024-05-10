from flask import Flask
from kafka import KafkaProducer
import json
import random
import time

app = Flask(__name__)

# Connect to Kafka server
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Sample product data
products = [
    {"id": 1, "name": "Product 1", "price": 10},
    {"id": 2, "name": "Product 2", "price": 20},
    {"id": 3, "name": "Product 3", "price": 30},
  
]

if __name__ == '__main__':
    while True:
        # Simulate generating e-commerce data for orders
        order = {
            "order_id": random.randint(1, 1000),
            "customer_id": random.randint(1, 100),
            "products": random.choices(products, k=random.randint(1, 5)),
            "timestamp": int(time.time())
        }
        
        # Send order data to Kafka topic
        producer.send('orders', value=order)
        print("Sent order:", order)
        time.sleep(1)  # Simulate real-time data
