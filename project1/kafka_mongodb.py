from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

MIN_LOAD_COUNT = 100

def load_to_mongodb(data, collection):
    try:
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into MongoDB")
    except Exception as e:
        print(f"Error inserting records into MongoDB: {e}")

if __name__ == '__main__':
    consumer_config = {
        'bootstrap.servers': os.getenv('LOCALHOST_BOOTSTRAP_SERVERS'),
        'sasl.username': os.getenv('LOCALHOST_SASL_USERNAME'),
        'sasl.password': os.getenv('LOCALHOST_SASL_PASSWORD'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISMS'),
        'group.id': os.getenv('LOAD_MONGODB_GROUP_ID'),
        'enable.auto.commit': os.getenv('CONSUMER_ENABLE_AUTO_COMMIT').lower() == 'true',
        'auto.offset.reset': os.getenv('CONSUMER_AUTO_OFFSET_RESET'),
    }

    # Set up a consumer to read data
    consumer = Consumer(consumer_config)

    topic = ['TwoParTopic']
    consumer.subscribe(topic)

    # Set up a MongoDB client to load data
    client = MongoClient(os.getenv('MONGODB_URI'))
    db = client[os.getenv('MONGODB_DB')]
    collection = db[os.getenv('MONGODB_COLLECTION')]

    try:
        msg_count = 0
        batch_data = []
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                # Process messages
                key = msg.key().decode('utf-8') if msg.key() is not None else None
                value = msg.value().decode('utf-8') if msg.value() is not None else None
                print(f"Load event from topic {msg.topic()}: key = {key}, offset = {msg.offset()}")

                # Load to MongoDB
                batch_data.append(json.loads(value))
                msg_count += 1
                if msg_count % MIN_LOAD_COUNT == 0:
                    load_to_mongodb(batch_data, collection)
                    batch_data = []  # Reset the batch data
                    consumer.commit(asynchronous=False)
    
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Load any remaining data to MongoDB
        if batch_data:
            load_to_mongodb(batch_data, collection)
        consumer.close()
