import faust
import os
from models import create_random_message
from consumer import consumer

# faust_example buffer example
# https://faust.readthedocs.io/en/latest/playbooks/vskafka.html

# faust_example Buffer up many events at a time
# https://faust.readthedocs.io/en/latest/playbooks/cheatsheet.html
# async for orders_batch in orders.take(100, within=30.0):
#     print(len(orders))

# faust_example when agent raise an error
# https://faust.readthedocs.io/en/latest/userguide/agents.html#when-agents-raise-an-error

# faust_example Acknowledgment
# https://faust.readthedocs.io/en/latest/userguide/streams.html#acknowledgment


MESSAGES_TOPIC = os.environ.get('MESSAGES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND

app = faust.App('producer', broker=KAFKA_BROKER_URL)

topic = app.topic(MESSAGES_TOPIC)


@app.timer(interval=SLEEP_TIME)
async def producer():
    message = create_random_message()
    await consumer.send(
        key=str(message.instance),
        value=message
    )


if __name__ == '__main__':
    app.main()