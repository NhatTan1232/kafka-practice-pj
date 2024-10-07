# Kafka and MongoDB Integration

This project demonstrates how to integrate Kafka with MongoDB using Python. It includes a Kafka consumer that reads messages from a Kafka topic and stores them in a MongoDB collection.

## Prerequisites

Ensure you have the following installed:
- Python 3.6+
- Kafka
- MongoDB
- Virtualenv (recommended for managing dependencies)

## Installation

1. **Set up a virtual environment:**
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

2. **Create a `.env` file:**
    Create a `.env` file in the project root directory and add the following configurations:
    ```dotenv
    # Consumer Configuration
    SOURCE_BOOTSTRAP_SERVERS=<SOURCE_SERVER>
    SOURCE_SASL_USERNAME=<SOURCE_USER>
    SOURCE_SASL_PASSWORD=<SOURCE_PASSWORD>
    SOURCE_GROUP_ID=<YOUR_CONSUMER_GROUP_ID>
    SOURCE_ENABLE_AUTO_COMMIT=False
    SOURCE_AUTO_OFFSET_RESET=earliest

    # Localhost Kafka Configuration
    LOCALHOST_BOOTSTRAP_SERVERS=<YOUR_LOCAL_KAFKA>
    LOCALHOST_SASL_USERNAME=<YOUR_LOCAL_USER>
    LOCALHOST_SASL_PASSWORD=<YOUR_LOCAL_PASSWORD>
    LOAD_MONGODB_GROUP_ID=<YOUR_LOCAL_CONSUMER_GROUP_ID>

    # Fixed properties
    SECURITY_PROTOCOL=SASL_PLAINTEXT
    SASL_MECHANISMS=PLAIN

    # MongoDB Configuration
    MONGODB_URI=<YOUR_MONGODB_URI>
    MONGODB_DB=<YOUR_MONGODB_DATABASE_NAME>
    MONGODB_COLLECTION=<YOUR_MONGODB_COLLECTION_NAME>
    ```

## Usage

### Start the Kafka Consumer
To start the Kafka consumer that reads data from the source Kafka topic and stores it in your Kafka topic, run the following command:

```sh
python src_kafka.py
```

To start the Kafka consumer that reads data from the Kafka topic and stores it in MongoDB, run the following command:

```sh
python kafka_mongodb.py
