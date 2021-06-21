"""Produce fake transactions into a Kafka topic."""

import os
from time import sleep
import json
import random

from kafka import KafkaProducer

MESSAGES_TOPIC = os.environ.get('MESSAGES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND

tenant_id = 1000
instance_id_list = list(i for i in range(100))

message_counter = 0

def create_random_message():
    global message_counter
    instance = random.choice(instance_id_list)
    message = {
        "instance": instance,
        "resources": [
            {
                "key": message_counter
            }
        ]
    }
    message_counter += 1
    return message


if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    while True:
        message = create_random_message()
        producer.send(
            MESSAGES_TOPIC,
            key=str(message["instance"]).encode(),
            value=message
        )
        print(message)  # DEBUG
        sleep(SLEEP_TIME)
