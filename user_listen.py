#!/usr/bin/env python

import sys
import MySQLdb as sql
import pika
import string

# Debug spy connects to EVERYTHING and dumps to console to assist debugging

def keyval_get(chunk, key, defaultValue=""):
    parts = string.split(chunk)
    for X in range(0, len(parts)):
        y = string.split(parts[X], '=')
        if (y[0] == key) & (len(y) > 1):
                return y[len(y)-1]
    return defaultValue


RMQHOST = os.environ['STREAMBOSS_RABBITMQ_HOST']

E_STREAM_NAMETAKEN = -100;

STREAMSTATE = {'COLD' : 0, 'COOL' : 1, 'WARM' : 2, 'HOT' : 3}

rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RMQHOST))
channel = rmq_conn.channel()

myqueue = channel.queue_declare('')

# subscribe to announcer queue
channel.queue_bind(exchange='stream_announce', queue=myqueue.method.queue)

print "Stream to monitor? ",
userstream = sys.stdin.readline()

channel.queue_bind(exchange='streams', queue=myqueue.method.queue, routing_key=userstream);


def loggerfunc(ch, method, properties, body):
    if method.exchange == "stream_announce":
        print "Message on stream_announce: %s - %s" % (method.routing_key, body)
    else:
         print "Got message from %s: %s" % (userstream, body)

channel.basic_consume(loggerfunc, queue=myqueue.method.queue, no_ack=True)

try:
    channel.start_consuming()
except:
    channel.stop_consuming()

rmq_conn.close()

