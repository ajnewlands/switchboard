#!/usr/bin/env python3

#
# Listens for ViewStart, ViewEnd, ViewAck
# Will dispatch ViewUpdate in response to ViewStart and ViewAck
#

import pika
import flatbuffers
import switchboard.ViewUpdate as ViewUpdate
import switchboard.ViewAck as ViewAck
import switchboard.ViewStart as ViewStart
import switchboard.ViewEnd as ViewEnd
from switchboard.Msg import Msg

connection = pika.BlockingConnection( pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='view')
channel.queue_bind(queue='view', exchange='switchboard', routing_key='')


def callback(ch, method, properties, body):
    print("Headers %r" % properties.headers)
    session = properties.headers["session"]
    message = Msg.GetRootAsMsg(body, 0)
    print("Got message for session %s", session);


channel.basic_consume(queue='view', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
