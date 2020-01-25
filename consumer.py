from confluent_kafka import Consumer, KafkaException, KafkaError
from protobuf.data.ImageData_pb2 import Image
import sys

conf = {'bootstrap.servers': "localhost:9092",
        'auto.offset.reset': 'smallest',
        'group.id': 'test'}

consumer = Consumer(conf)

running = True

try:
    consumer.subscribe(['VideoStream'])

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print(msg.value().decode('ascii').split('\n')[1])
finally:
# Close down consumer to commit final offsets.
    consumer.close()
        