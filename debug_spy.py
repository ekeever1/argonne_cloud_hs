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


DBHOST = os.environ['STREAMBOSS_DBHOST']
DBUSER = os.environ['STREAMBOSS_DBUSER']
DBPASS = os.environ['STREAMBOSS_DBPASS']
DBDB = os.environ['STREAMBOSS_DBNAME']

RMQHOST = os.environ['STREAMBOSS_RABBITMQ_HOST']

E_STREAM_NAMETAKEN = -100;

STREAMSTATE = {'COLD' : 0, 'COOL' : 1, 'WARM' : 2, 'HOT' : 3}

rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RMQHOST))
channel = rmq_conn.channel()

myqueue = channel.queue_declare('')

db_conn = sql.connect(DBHOST, DBUSER, DBPASS, DBDB)
curs = db_conn.cursor()

nRows = curs.execute("SELECT * FROM datastreams")

# subscribe to announcer queue
channel.queue_bind(exchange='stream_announce', queue=myqueue.method.queue)

for j in range(0, nRows):
    dbvals = curs.fetchone()
    print "Subscribing to %s..." % (dbvals[2])
    channel.queue_bind(exchange='streams', queue=myqueue.method.queue, routing_key=dbvals[2]);

if nRows == 0:
    print "No streams currently exist to subscribe to"

def loggerfunc(ch, method, properties, body):
    if method.exchange == "stream_announce":
        print "Got msg on queue %s with key %s: %s" % (method.exchange, method.routing_key, body)

        if body[0] == 'N': # New stream: subscribe
            thekey = keyval_get(body, "key", "INVALID")
	    if thekey == "INVALID":
                print "Malformed [N] message on announce queue!"
            else:
                channel.queue_bind(exchange='streams', queue=myqueue.method.queue, routing_key=thekey);
                print "New stream announced; Subscribing to %s" % (thekey)

	if body[0] == 'D': # Dying stream, unsubscribe
            parts = string.split(body);
            if len(parts) == 2:
                thekey = parts[1]
                channel.queue_unbind(exchange='streams', queue=myqueue.method.queue, routing_key=thekey);
                print "Stream death announced; Unsubscribing from %s" % (thekey)
            else:
                print "Malformed [D] message on announce queue: "+body
    else:
         print "Got msg from stream %s: %s" % (method.routing_key, body)

channel.basic_consume(loggerfunc, queue=myqueue.method.queue, no_ack=True)

try:
    channel.start_consuming()
except:
    channel.stop_consuming()

rmq_conn.close()

