import os
import faust

# this model describes how message values are serialized
# in the Kafka "orders" topic.
class Transaction(faust.Record, serializer='json'):
    source: str
    target: str
    amount: int
    currency: str


KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
LEGIT_TOPIC = os.environ.get('LEGIT_TOPIC')
FRAUD_TOPIC = os.environ.get('FRAUD_TOPIC')


def is_suspicious(transaction: Transaction) -> bool:
    """Determine whether a transaction is suspicious."""
    return transaction.amount >= 900


app = faust.App('hello-app', broker=KAFKA_BROKER_URL, topic_partitions=10)
transactions_topic = app.topic(TRANSACTIONS_TOPIC, value_type=Transaction, partitions=10)
fraud_topic = app.topic(FRAUD_TOPIC, value_type=Transaction)
legit_topic = app.topic(LEGIT_TOPIC, value_type=Transaction)

@app.agent(fraud_topic)
async def fraud_process(Transactions):
    async for Transaction in Transactions:
        print(f'fraud {Transaction}')

@app.agent(legit_topic)
async def legit_process(Transactions):
    async for Transaction in Transactions:
        print(f'legit {Transaction}')


# our table is sharded amongst worker instances, and replicated
# with standby copies to take over if one of the nodes fail.
# partition number should be exactly same as Transaction.
order_count_by_account = app.Table(
    'order_count', default=int, partitions=10)

@app.agent(transactions_topic)
async def process(orders: faust.Stream[Transaction]) -> None:
    async for order in orders:
        order_count_by_account[order.source] += 1
        producer = (
            fraud_process if is_suspicious(order) else legit_process)
        await producer.send(value=order)

if __name__ == '__main__':
    app.main()