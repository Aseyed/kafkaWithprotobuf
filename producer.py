from confluent_kafka import Producer
from protobuf.data.data_pb2 import Msg
import socket
import sys
import logging
import base64

logging.basicConfig(level=logging.DEBUG)
conf = {'bootstrap.servers': "localhost:9092"}

producer = Producer(conf)

def acked(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

image = open("img.png", "rb")
f = image.read()
image.close()
# myBytes = bytearray(f)

my_message = Msg(
    data = f,
    app_name = 'img',
    file_ext = 'png'
)

for frame in range(10):
    my_message.frame = frame
    # producer.send(test_image)
    myObject = base64.b64encode(my_message.SerializeToString())
    res = producer.produce('VideoStream',myObject, callback=acked)
    print(res)
    
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.

    producer.poll(1)


