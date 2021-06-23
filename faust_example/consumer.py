import faust
import os
from models import Message
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

# faust offset known issue (where offset lag is always 1)
# https://github.com/robinhood/faust/issues/73

MESSAGES_TOPIC = os.environ.get('MESSAGES_TOPIC')
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')

app = faust.App('consumer', broker=KAFKA_BROKER_URL)

topic = app.topic(MESSAGES_TOPIC, value_type=Message)


@app.agent(topic)
async def consumer(messages):
    # Do batching, at most 10 message, time window 10 seconds
    async for message in messages.take(10, within=10):
        print(message)

if __name__ == '__main__':
    app.main()
