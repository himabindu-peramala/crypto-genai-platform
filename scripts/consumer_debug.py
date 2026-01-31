from kafka import KafkaConsumer
import json

TOPIC_NAME = 'crypto_market_data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def run_consumer():
    print(f"Listening to {TOPIC_NAME}...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        print("Stopping consumer...")

if __name__ == "__main__":
    run_consumer()
