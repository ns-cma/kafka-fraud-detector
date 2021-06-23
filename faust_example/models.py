import faust
import typing
import random


class Message(faust.Record, validation=True):
    instance: int
    # too complicated type like this won't be validated from what I saw
    resources: typing.List[dict]


instance_id_list = list(i for i in range(100))
message_counter = 1


def create_random_message():
    global message_counter
    instance = random.choice(instance_id_list)
    message = Message(
        instance=instance,
        resources=[{"key": message_counter}]
    )
    message_counter += 1
    return message
