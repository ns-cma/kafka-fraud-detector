"""Example Kafka consumer."""

import json
import os

from confluent_kafka import Consumer

MESSAGES_TOPIC = os.environ.get('MESSAGES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


if __name__ == '__main__':
    consumer = Consumer(
        {'bootstrap.servers': KAFKA_BROKER_URL,
         'group.id': "my_consumer_group",
         'session.timeout.ms': 6000,
         'auto.offset.reset': 'earliest'}
    )

    consumer.subscribe([MESSAGES_TOPIC], on_assign=print_assignment)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('%% %s [%d] at offset %d with key %s:\n' %
                  (msg.topic(), msg.partition(), msg.offset(),
                   str(msg.key())))

            print(msg.value())
    finally:
        consumer.close()
