from kafka import KafkaConsumer, KafkaProducer
from src.kafka_to_kafka.yaml_config import load_config
import logging
import json

config = load_config()
KAFKA_TOPIC = config["KAFKA_TOPIC"]
SOURCE_BOOTSTRAP_SERVERS = config["SOURCE_BOOTSTRAP_SERVERS"]
LOCAL_BOOTSTRAP_SERVERS = config["LOCAL_BOOTSTRAP_SERVERS"]
SECURITY_PROTOCOL = config["SECURITY_PROTOCOL"]
SASL_MECHANISM = config["SASL_MECHANISM"]
SASL_PLAIN_USERNAME = config["SASL_PLAIN_USERNAME"]
SASL_PLAIN_PASSWORD = config["SASL_PLAIN_PASSWORD"]

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

def collect_message():
    # Initialize consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers = SOURCE_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id='kafka_to_kafka_consumer',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
            **get_sasl_config()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")

    # Initialize producer
    try:
        producer = KafkaProducer(
            bootstrap_servers= LOCAL_BOOTSTRAP_SERVERS,
            value_serializer=lambda m: json.dumps(m).encode('ascii'),
            **get_sasl_config()
        )
    except Exception as e:
        logging.exception(f"Error when connecting to kafka server")

    count = 0

    # Consuming message and produce to local kafka
    try:
        for message in consumer:
            producer.send('product_view', message.value)
            producer.flush()
            count+=1
            logging.info(f"Have saved {count}")
    except Exception as e:
        logging.exception(f"Interruption")
    finally:
        consumer.close()
        producer.close()
        logging.info(f"Have done")