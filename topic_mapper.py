from confluent_kafka import Consumer, Producer
from uuid import uuid1
import argparse
import attr
from typing import List
from multiprocessing import Process


@attr.s
class Mapping:
    input_topics = attr.ib(type=List[str, ...])  # List rather than Tuple as that is what Consumer.subscribe() takes
    output_topic = attr.ib(type=str)


def print_assignment(_, partitions):
    print('Assignment:', partitions)


def forward_messages(broker: str, mapping: Mapping):
    consumer_conf = {'bootstrap.servers': broker, 'group.id': uuid1(), 'session.timeout.ms': 6000,
                     'auto.offset.reset': 'latest'}
    consumer = Consumer(consumer_conf)

    producer_conf = {'bootstrap.servers': args.broker}
    producer = Producer(**producer_conf)

    subscribed = False
    while not subscribed:
        try:
            consumer.subscribe(mapping.input_topics, on_assign=print_assignment)
        except:
            continue
        subscribed = True

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
        else:
            producer.produce(mapping.output_topic, msg.value())
            producer.poll(timeout=1.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost:9092")
    args = parser.parse_args()

    mappings = [Mapping(input_topics=["V20_linearMotion", "V20_timingStatus"], output_topic="V20_sampleEnv")]

    for mapping in mappings:
        Process(target=forward_messages, args=(args.broker, mapping)).start()
