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


class handler(object):
    def __init__(self):
        self.started = 0
        self.sqn = 0

    def dispatchMsg(self, dest_id, session_id, msg):
        channel.basic_publish(
            exchange='switchboard', 
            routing_key='', 
            properties = pika.BasicProperties(
                headers = { 'dest_id': dest_id, 'type': 'ViewUpdate', 'session': session_id }
                ),
            body=msg)

    def getViewUpdateMsg(self):
        builder =flatbuffers.Builder(1024) # python is missing a Clear() method to reuse?
        ViewUpdate.ViewUpdateStart(builder)
        ViewUpdate.ViewUpdateAddSqn(builder, self.sqn)
        self.sqn +=1
        ViewUpdate.ViewUpdateAddIncremental(builder, False)
        viewupdate = ViewUpdate.ViewUpdateEnd(builder)

        Msg.MsgStart(builder)
        Msg.MsgAddContentType(builder, Content.Content().ViewUpdate)
        Msg.MsgAddContent(builder, viewupdate)
        msg = Msg.MsgEnd(builder)

        builder.Finish(msg)
        return builder.Output() 




    def callback(self, ch, method, properties, body):
        if (self.started == 0):
            self.started = time.time()
            print("Started test")
        #print("Headers %r" % properties.headers)
        session = properties.headers["session"]
        sender = properties.headers["sender_id"]
        message = Msg.Msg.GetRootAsMsg(body, 0)
        #print("Got message for session ", session);
        view = self.getViewUpdateMsg()
        self.dispatchMsg(sender, session, view)
        if (self.sqn == 1000):
            end_time = time.time()
            delta_ms = (end_time - self.started) * 1000
            ops_second = (self.sqn / delta_ms) * 1000
            print("Completed after %f ms, %f round trips per second" %( delta_ms, ops_second ))
            sys.exit(0)


if __name__== "__main__":
    connection = pika.BlockingConnection( pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    handler = handler();

    channel.queue_declare(queue='view', auto_delete=True)
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewStart'})
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewAck'})
    channel.queue_bind(queue='view', exchange='switchboard', routing_key='', arguments={'type': 'ViewEnd'})

    channel.basic_consume(queue='view', on_message_callback=handler.callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


