"""Batch aggregator sample code for reference (untested)"""
import os
import json
from confluent_kafka import Consumer, Producer

# confluent_kafka batch consumer example
# https://github.com/confluentinc/confluent-kafka-python/blob/e671bccb8a4f98302748ccf60d5d579f68c6613d/tests/integration/integration_test.py#L592

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')

if __name__ == '__main__':
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER_URL
    })

    consumer = Consumer(
        {'bootstrap.servers': KAFKA_BROKER_URL,
         'group.id': "my_consumer_group",
         'session.timeout.ms': 6000,
         'enable.auto.commit': False,
         'auto.offset.reset': 'earliest'}
    )
    consumer.subscribe(["processed_event_queue"])
    try:
        while True:
            # try to accumulate up to 1000 events within 10 minutes
            msglist = consumer.consume(1000, timeout=600.0)
            if not msglist:  # no events in 10 mins
                continue

            instance_operation_map = {}
            # format as following:
            # {
            #     "<instance_id>": {
            #         "upsert": <list of resource metadata> [list of dict],
            #         "delete": <list of resource metadata> [list of dict]
            #     }
            # }

            # batching events via instance_operation_map
            for msg in msglist:
                instance_operation_map.setdefault(
                    msg['instance'],
                    {'upsert': [], 'delete': []}
                )
                for resource_event in msg['events']:
                    event_type = resource_event['type']
                    resource = resource_event['resource']
                    if event_type in ("create", "update"):
                        instance_operation_map['upsert'].append(resource)
                    elif event_type == 'delete':
                        instance_operation_map['delete'].append(resource)
                    else:
                        print('unknown event type')

            # prepare and send events to Request Queue
            batch_size = 50
            for instance_id, operations in instance_operation_map.items():
                request_events = []
                if operations['upsert']:
                    resources = operations['upsert']
                    for i in range(0, len(resources), batch_size):
                        batch_resources = resources[i:i+batch_size]
                        request_events.append({
                            'method': 'PUT',
                            'body': {
                                'instance': instance_id,
                                'Resources': batch_resources
                            }
                        })
                if operations['delete']:
                    for delete_resource in operations['delete']:
                        request_events.append({
                            'method': 'DELETE',
                            'body': delete_resource
                        })

                producer.produce(
                    "request_queue",
                    key=str(instance_id).encode(),
                    value=json.dumps(request_events)
                )

            # commit offset after batch processing
            consumer.commit(msglist[-1], asynchronous=False)
    finally:
        consumer.close()
        producer.flush()
