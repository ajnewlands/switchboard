#!/usr/bin/env python3
import pika

connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
channel.queue_bind(queue='hello', exchange='switchboard', routing_key='', arguments={'dest_id':'97060b13-292a-4372-9cd5-830b28064eb7'})


def callback(ch, method, properties, body):
        print(" [x] Received %r" % properties.headers)
        print(" [x] Received %r" % body)

channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
