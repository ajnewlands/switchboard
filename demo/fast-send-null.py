#!/usr/bin/env python3

#
# Listens for ViewStart, ViewEnd, ViewAck
# Will dispatch ViewUpdate in response to ViewStart and ViewAck
#

import sys
import pika
import time as time
import flatbuffers
import switchboard.ViewUpdate as ViewUpdate
import switchboard.ViewAck as ViewAck
import switchboard.ViewStart as ViewStart
import switchboard.ViewEnd as ViewEnd
import switchboard.Msg as Msg
import switchboard.Content as Content

message_target = 10000

class handler(object):
    def __init__(self):
        self.started = 0
        self.sqn = 0
        self.premade_message = None
        self.received = 0

    def dispatchMsg(self, dest_id, session_id, msg):
        channel.basic_publish(
            exchange='switchboard', 
            routing_key='', 
            properties = pika.BasicProperties(
                headers = { 'dest_id': dest_id, 'type': 'ViewUpdate', 'session': session_id }
                ),
            body=msg)

    def getViewUpdateMsg(self, sqn):
        builder =flatbuffers.Builder(1024) # python is missing a Clear() method to reuse?
        ViewUpdate.ViewUpdateStart(builder)
        ViewUpdate.ViewUpdateAddSqn(builder, sqn)
        ViewUpdate.ViewUpdateAddIncremental(builder, False)
        viewupdate = ViewUpdate.ViewUpdateEnd(builder)

        Msg.MsgStart(builder)
        Msg.MsgAddContentType(builder, Content.Content().ViewUpdate)
        Msg.MsgAddContent(builder, viewupdate)
        msg = Msg.MsgEnd(builder)

        builder.Finish(msg)
        return builder.Output() 




    def callback(self, ch, method, properties, body):
        mtype = properties.headers["type"]
        if (mtype == "ViewStart"):
            self.started = time.time()
            print("Started test")
            sent = 0
            while( sent < 10000):
                session = properties.headers["session"]
                sender = properties.headers["sender_id"]
                message = Msg.Msg.GetRootAsMsg(body, 0)
                self.dispatchMsg(sender, session, self.premade_message)
                sent += 1
            print("sent 10000")
        elif( mtype == "ViewAck" ): 
            self.received += 1
            if( self.received == 10000 ):
                end_time = time.time()
                delta_ms = (end_time - self.started) * 10000
                ops_second = (self.received / delta_ms) * 10000
                print("Completed after %f ms, %f round trips per second" %( delta_ms, ops_second ))
                sys.exit(0)


if __name__== "__main__":
    connection = pika.BlockingConnection( pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=30)
    handler = handler();
    handler.premade_message = handler.getViewUpdateMsg(0)

    channel.queue_declare(queue='view', auto_delete=True, durable=False)
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewStart'})
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewAck'})
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewEnd'})

    channel.basic_consume(queue='view', on_message_callback=handler.callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


