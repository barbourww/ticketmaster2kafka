import asyncio
import json
import os

from confluent_kafka import Producer

import logging
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()


class KafkaConfluentHelper:
    def __init__(self, config):
    
        self.conf = {
            'bootstrap.servers': config['KAFKA_BOOTSTRAP'],
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': 'strimzi-ca.crt',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': config['KAFKA_USER'],
            'sasl.password': config['KAFKA_PASSWORD'],
            'ssl.endpoint.identification.algorithm': 'none'
        }

        self.producer = Producer(self.conf)

        # Fetch and print broker metadata
        md = self.producer.list_topics(timeout=10)

        print("Brokers:")
        for broker in md.brokers.values():
            print(f"- id: {broker.id}, host: {broker.host}, port: {broker.port}")

    def on_send_success(self, record_metadata):
        pass

    def on_send_error(self, excp):
        logger.error('I am an errback', exc_info=excp)
        
    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Delivery failed: {err}')

    def send(self, topic, key,  json_data, partition=None, headers=None):
        self.producer.produce(topic,
                              key=key.encode('utf-8'),
                              value=json.dumps(json_data).encode('utf-8'),
                              headers=headers,
                              callback=self.delivery_report)
        self.producer.poll(0)


if __name__ == "__main__":
    common_kafka_config = {
        'KAFKA_BOOTSTRAP': os.environ.get('KAFKA_BOOTSTRAP'),
        'KAFKA_USER': os.environ.get('KAFKA_USER'),
        'KAFKA_PASSWORD': os.environ.get('KAFKA_PASSWORD'),
    }
    kc = KafkaConfluentHelper(common_kafka_config)

    new_data = {
        'test': "hello"
    }
    headers = [('key1', b'value1'), ('key2', b'value2')]
    kc.send(topic='postgres-data', key='my_key', partition=0, json_data=new_data, headers=headers)
    
