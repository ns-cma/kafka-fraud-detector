"""Produce fake transactions into a Kafka topic."""

import os
from time import sleep
import json
import random

from confluent_kafka import Producer

# JSON producer
# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_producer.py

# JSON consumer
# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/json_consumer.py


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
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER_URL
    })
    while True:
        message = create_random_message()
        producer.produce(
            MESSAGES_TOPIC,
            key=str(message["instance"]).encode(),
            value=json.dumps(message)
        )
        print(message)  # DEBUG
        sleep(SLEEP_TIME)
