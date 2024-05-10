from kafka import KafkaConsumer
import json

# Connect to Kafka server
consumer = KafkaConsumer('orders', bootstrap_servers='localhost:9092', value_deserializer=lambda v: json.loads(v.decode('utf-8')))

try:
    print("Consumer started successfully.")
    for message in consumer:
        order = message.value
        
        # Process order data
        total_price = sum(product["price"] for product in order["products"])
        
        # Perform further processing or analytics
        print("Received order:", order)
        print("Total price:", total_price)

except Exception as e:
    print("An error occurred:", str(e))

finally:
    # Close the Kafka consumer connection
    consumer.close()
