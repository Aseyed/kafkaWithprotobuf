from confluent_kafka import Producer
from protobuf.data.ImageData_pb2 import Image
import socket
import sys
import logging

logging.basicConfig(level=logging.DEBUG)
conf = {'bootstrap.servers': "localhost:9092"}

producer = Producer(conf)

def acked(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))


test_image = Image(
    image_data = b"this is just a test string",
    height = 10,
    width = 10
)
for frame in range(100):
    test_image.frame = frame
    # producer.send(test_image)
    myObject = test_image.SerializeToString()
    producer.produce('VideoStream',myObject, callback=acked)
    
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.

    producer.poll(1)


