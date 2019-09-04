from confluent_kafka import Consumer, Producer
from uuid import uuid1
import argparse
import attr
from typing import List, Optional
from multiprocessing import Process


@attr.s
class Mapping:
    input_topics = attr.ib(type=List[str])  # List rather than Tuple as that is what Consumer.subscribe() takes
    output_topic = attr.ib(type=str)
    filter_schema = attr.ib(type=Optional[bytes], default=None)


def print_assignment(_, partitions):
    print('Assignment:', partitions)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def forward_messages(broker: str, mapping: Mapping):
    consumer_conf = {'bootstrap.servers': broker, 'group.id': uuid1(), 'session.timeout.ms': 6000,
                     'auto.offset.reset': 'latest', 'api.version.request': True}
    consumer = Consumer(consumer_conf)

    producer_conf = {'bootstrap.servers': args.broker, 'api.version.request': True}
    producer = Producer(**producer_conf)

    subscribed = False
    while not subscribed:
        try:
            consumer.subscribe(mapping.input_topics, on_assign=print_assignment)
        except:
            continue
        subscribed = True

    while True:
        print("Poll for message")
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
        else:
            if mapping.filter_schema is not None and msg.value()[4:8] == mapping.filter_schema:
                producer.produce(mapping.output_topic, msg.value(), timestamp=msg.timestamp()[1], callback=delivery_report)
            else:
                print(f"Message schema id was {msg.value()[4:8]}")
            producer.poll()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost:9092")
    args = parser.parse_args()

    mappings = [
        Mapping(input_topics=["V20_motion", "V20_detectorPower", "V20_timingStatus"], output_topic="V20_sampleEnv",
                filter_schema=b'f142'),
        Mapping(input_topics=["denex_detector", "monitor"], output_topic="V20_events",
                filter_schema=b'ev42')]

    # Start doing each topic mapping in parallel
    for mapping in mappings:
        Process(target=forward_messages, args=(args.broker, mapping)).start()
