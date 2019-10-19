#!/usr/bin/env python3
import sys
import pika
import flatbuffers
import switchboard.Broadcast as Broadcast
import switchboard.ViewUpdate as ViewUpdate
import switchboard.Content as Content
import switchboard.Msg as Msg

connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

builder = flatbuffers.Builder(1024)
broadcast_message = builder.CreateString('Hello world')
Broadcast.BroadcastStart(builder)
Broadcast.BroadcastAddText(builder, broadcast_message)
broadcast = Broadcast.BroadcastEnd(builder)

session = builder.CreateString(sys.argv[2])
Msg.MsgStart(builder)
Msg.MsgAddSession(builder, session)
Msg.MsgAddContentType(builder, Content.Content().Broadcast)
Msg.MsgAddContent(builder, broadcast)
msg = Msg.MsgEnd(builder)

builder.Finish(msg)
buf = builder.Output()

print("Publishing to dest id ", sys.argv[1])
channel.basic_publish(
        exchange='switchboard', 
        routing_key='', 
        properties = pika.BasicProperties(
            headers = { 'dest_id': sys.argv[1] }
            ),
        body=buf)
print(" [x] Sent message..")
connection.close()
