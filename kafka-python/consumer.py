"""Example Kafka consumer."""

import json
import os

from kafka import KafkaConsumer

MESSAGES_TOPIC = os.environ.get('MESSAGES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')


if __name__ == '__main__':
    consumer = KafkaConsumer(
        MESSAGES_TOPIC,
        group_id="my_consumer_group",
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    for message in consumer:
        print(message)

    # for batching messages we can use
    # while True:
    #     results = consumer.poll(timeout_ms=30 * 1000, max_records=1000)
    # consumer.commit(offsets=new_offset)
