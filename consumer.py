from confluent_kafka import Consumer, KafkaException, KafkaError
from protobuf.data.data_pb2 import Msg
import sys
import base64

conf = {'bootstrap.servers': "localhost:9092",
        'auto.offset.reset': 'smallest',
        'group.id': 'test'}

consumer = Consumer(conf)

running = True

theMessage = Msg()
try:
    consumer.subscribe(['VideoStream'])

    i = 0
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
            i += 1
            # print(base64.b64decode(msg.value()))
            theMessage.ParseFromString(base64.b64decode(msg.value()))
            f = open(f'/tmp/{theMessage.app_name}_{i}.{theMessage.file_ext}', 'wb')
            f.write(theMessage.data)
            f.close()
#            break
finally:
# Close down consumer to commit final offsets.
    consumer.close()

