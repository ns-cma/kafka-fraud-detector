import os
import faust
from transactions import create_random_transaction


# this model describes how message values are serialized
# in the Kafka "orders" topic.
class Transaction(faust.Record, serializer='json'):
    source: str
    target: str
    amount: int
    currency: str


KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND'))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

app = faust.App('hello-app', broker=KAFKA_BROKER_URL, topic_partitions=10)

transactions_topic = app.topic(TRANSACTIONS_TOPIC, value_type=Transaction, partitions=10)

@app.timer(SLEEP_TIME)
async def send_transaction():
    await transactions_topic.send(
        value=Transaction(**create_random_transaction()))


if __name__ == '__main__':
    app.main()