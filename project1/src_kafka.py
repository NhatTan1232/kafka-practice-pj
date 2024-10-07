from confluent_kafka import Producer, Consumer, KafkaError
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

MIN_COMMIT_COUNT = 500

def produce_message(producer, topic, key, value):
    producer.produce(topic, key=key, value=value)
    producer.flush()
    print('Flushed')

if __name__ == '__main__':

    consumer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv('SOURCE_BOOTSTRAP_SERVERS'),
        'sasl.username':     os.getenv('SOURCE_SASL_USERNAME'),
        'sasl.password':     os.getenv('SOURCE_SASL_PASSWORD'),

        # Fixed properties
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms':   os.getenv('SASL_MECHANISMS'),
        'group.id':          os.getenv('SOURCE_GROUP_ID'),
        'enable.auto.commit':os.getenv('SOURCE_ENABLE_AUTO_COMMIT'),
        'auto.offset.reset': os.getenv('SOURCE_AUTO_OFFSET_RESET'),
    }

    producer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv('LOCALHOST_BOOTSTRAP_SERVERS'),
        'sasl.username':     os.getenv('LOCALHOST_SASL_USERNAME'),
        'sasl.password':     os.getenv('LOCALHOST_SASL_PASSWORD'),

        # Fixed properties
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms':   os.getenv('SASL_MECHANISMS')
    }

    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    des_topic = 'TwoParTopic'
    topic = ['product_view']
    consumer.subscribe(topic)

    try:
        msg_count = 0
        while True:
            msg = consumer.poll(timeout=100.0)
            if msg is None:
                continue
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Decode keys and value if exists
                key = msg.key().decode('utf-8') if msg.key() is not None else None
                value = msg.value().decode('utf-8') if msg.value() is not None else None
                
                print(f"Consumed event from topic {msg.topic()}: offset = {msg.offset()}")

                # Produce message to topic
                produce_message(producer, des_topic, key, value)

                # Commit when reaches 500 message
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        producer.flush()


            