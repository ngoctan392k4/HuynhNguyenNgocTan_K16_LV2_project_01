from kafka import KafkaConsumer
from pymongo import MongoClient
from src.kafka_to_mongodb.yaml_config import load_config
import logging
import json
import time

config = load_config()
LOCAL_KAFKA_TOPIC = config["LOCAL_KAFKA_TOPIC"]
LOCAL_BOOTSTRAP_SERVERS = config["LOCAL_BOOTSTRAP_SERVERS"]
SECURITY_PROTOCOL = config["SECURITY_PROTOCOL"]
SASL_MECHANISM = config["SASL_MECHANISM"]
SASL_PLAIN_USERNAME = config["SASL_PLAIN_USERNAME"]
SASL_PLAIN_PASSWORD = config["SASL_PLAIN_PASSWORD"]
MONGODB_URL = config["MONGODB_URL"]
BATCH_SIZE = 500
MAX_WAIT_TIME = 5.0

#
# SASL configuration
#
def get_sasl_config():
    return {
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": SASL_PLAIN_USERNAME,
        "sasl_plain_password": SASL_PLAIN_PASSWORD
    }

def data_storage():
    # Initialize consumer
    try:
        consumer = KafkaConsumer(
            LOCAL_KAFKA_TOPIC,
            bootstrap_servers = LOCAL_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id='kafka_to_mongo_consumer',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            **get_sasl_config()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")

    # Initialize Mongodb connection
    try:
        client = MongoClient(MONGODB_URL)
        db = client['glamira_action']
        collection = db['product_view_v3']
        client.server_info()
        logging.info(f"Successfully connected to MongoDB at {MONGODB_URL}")
    except Exception as e:
        logging.exception(f"Error when connecting to MONGODB server: {e}")
        exit(1)

    # Storing data
    batch = []
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)

            if not messages:
                logging.warning("No new messages")
            else:
                for topic_partition, messages in messages.items():
                    for message in messages:
                        batch.append(message.value)

            if len(batch) >= BATCH_SIZE:
                try:
                    collection.insert_many(batch, ordered=False)
                    consumer.commit()
                    logging.info(f"Successfully inserted batch of {len(batch)} into MongoDB")

                    batch = []

                except Exception as e:
                    logging.exception(f"Error saving data to MongoDB. Retry after 5s")
                    time.sleep(5)

    except Exception as e:
        logging.info(f"Error: {e}")
    finally:
        if len(batch) > 0:
            try:
                collection.insert_many(batch, ordered=False)
                consumer.commit()
                logging.info(f"Successfully inserted batch of {len(batch)} into MongoDB")
            except Exception as e:
                logging.exception(f"Error saving final batch: {e}")

        consumer.close()
        client.close()

    # for message in consumer:
    #     data = message.value

    #     try:
    #         collection.insert_one(data)
    #     except Exception as e:
    #         logging.exception(f"Error when saving data into MONGODB: {e}")
