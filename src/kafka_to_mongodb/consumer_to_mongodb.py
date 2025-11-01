from kafka import KafkaConsumer
from pymongo import MongoClient
from src.kafka_to_mongodb.yaml_config import load_config
import logging
import json

config = load_config()
KAFKA_TOPIC = config["KAFKA_TOPIC"]
LOCAL_BOOTSTRAP_SERVERS = config["LOCAL_BOOTSTRAP_SERVERS"]
SECURITY_PROTOCOL = config["SECURITY_PROTOCOL"]
SASL_MECHANISM = config["SASL_MECHANISM"]
SASL_PLAIN_USERNAME = config["SASL_PLAIN_USERNAME"]
SASL_PLAIN_PASSWORD = config["SASL_PLAIN_PASSWORD"]
MONGODB_URL = config["MONGODB_URL"]

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
            KAFKA_TOPIC,
            bootstrap_servers = LOCAL_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id='kafka_to_mongo_consumer',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            **get_sasl_config()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")

    # Initialize Mongodb connection
    try:
        client = MongoClient(MONGODB_URL)
        db = client['glamira_action']
        collection = db['product_view']
        client.server_info()
        logging.info(f"Successfully connected to MongoDB at {MONGODB_URL}")
    except Exception as e:
        logging.exception(f"Error when connecting to MONGODB server: {e}")
        exit(1)

    # Storing data
    for message in consumer:
        data = message.value

        try:
            collection.insert_one(data)
        except Exception as e:
            logging.exception(f"Error when saving data into MONGODB: {e}")
