#!/usr/bin/env python

# announcer.py: author Erik Keever
# Sends messages to all listeners when a new stream requests "officiality"

import os
import string
import uuid

import pika
import MySQLdb as sql

#----------
def handle_streamreq(ch, method, props, body):
    print "handle_announce got msg: "+body
    first  = body[0]

    if (first == 'N'): # New; Expect: [N | name=STRING | freq=FLOAT process=PROCNAME]
        key = keyval_get(body, "key", "INVALID")
        thefreq = keyval_get(body, "freq", "0")
        theproc = keyval_get(body, "process", "printit")

        # Reply no if malformed
        if key == "INVALID":
            channel.basic_publish(exchange='', routing_key=props.reply_to, 
                properties = pika.BasicProperties(correlation_id =  props.correlation_id), body="N")
        
        # Try to create the stream and return the reply
        reply = instream_createNew(key, thefreq, theproc, props.correlation_id)

        print "Reply to new: "+reply
        channel.basic_publish(exchange='', routing_key=props.reply_to,
             properties = pika.BasicProperties(correlation_id = props.correlation_id), body=reply)
        return

    if (first == 'A'): # Alter; Require: [A | uuid | (freq=%f | pauseme=TF)]
        sparts = string.split(body, ' ')
        if len(sparts) > 2:
            theuuid = s[1]
            nLines = db_curs.execute("SELECT routekey FROM datastreams WHERE tx_uuid = '%s'" % (theuuid) )
            if nLines == 1: # valid
                dropme = db_curs.fetchone();
                newfreq = keyval_get(body, "freq", "-1.0")
                pauseit = keyval_get(body, "pauseme", "-1")
                
                result = instream_updateExisting(theuuid, newfreq, pauseit)
            else:
                result = "N"

        print "Reply to alter: "+reply
        channel.basic_publish(exchange='', routing_key=props.reply_to,
             properties = pika.BasicProperties(correlation_id =  props.correlation_id), body=reply)

        return;

    if (first == 'R'): # Reconnect; Require: [R original_uuid]
        sparts = string.split(body, ' ')
        if len(sparts) == 2:
            myreply = instream_reconnect(sparts[1], props.correlation_id);
            print "Reply to reconnect: "+myreply
            channel.basic_publish(exchange='', routing_key=props.reply_to,
                properties = pika.BasicProperties(correlation_id =  props.correlation_id), body=myreply)

        return;

    if (first == 'D'): # Delete; Require: [D | uuid_key | action=<kill | cool>]
        sparts = string.split(body, ' ')
        if len(sparts) == 3:
            act = keyval_get(body, "action", "INVALID")
            reply = "N"
            if act == "kill":
		reply = instream_disconnect(sparts[1], 0);
	    if act == "cold":
		reply = instream_disconnect(sparts[1], 1);
        else:
            reply = "N"

        print "Reply to delete: "+reply
        channel.basic_publish(exchange='', routing_key=props.reply_to,
                properties = pika.BasicProperties(correlation_id =  props.correlation_id), body=reply)

        return

    print "Warning: invalid message="+body
    return

#---------- Utility functions
def keyval_get(chunk, key, defaultValue=""):
    parts = string.split(chunk)
    for X in range(0, len(parts)):
        y = string.split(parts[X], '=')
        if (y[0] == key) & (len(y) > 1):
                return y[len(y)-1]
    return defaultValue

def announceMsg(text):
    channel.basic_publish(exchange='stream_announce',routing_key='',body=text)

#---------- Create/Destroy functions

def instream_createNew(routekey, freq, processor, remotes_uuid):
    qtext = "SELECT routekey FROM datastreams WHERE (routekey = '%s');" % (routekey)
    nLines = db_curs.execute(qtext)

    if (nLines > 0):
        return "N"

    # Generate a random 63-bit number as the streamid
    # This is (I believe) "safe" and non-guessable and should be a reliable key.
    newuuid = uuid.uuid4().int / 36893488147419103232

    # Insert new stream into stream database
    # db schema: [tx_uuid, rx_uuid, streamname, status, process_name, freq, israw]
    qtext = "INSERT INTO datastreams (tx_uuid, rx_uuid, routekey, status, process_name, freq, israw) VALUES (%i, %s, '%s', %i, '%s', %s, %i);" % (newuuid, remotes_uuid, routekey, STREAMSTATE['WARM'], processor, freq, 1)
    nLines = db_curs.execute(qtext)

    if nLines != 1:
        return "N"

    # Announce on the stream_announce fanout:
    announceMsg( "N key=%s freq=%s state=%i" % (routekey, freq, STREAMSTATE['WARM']) )
    # Return the string we return to the requester's RPC
    return "Y %i %s" % (newuuid, routekey)

#----------
def instream_reconnect(theuuid, remote_uuid):
    qtext = "SELECT * FROM datastreams WHERE tx_uuid = %s;" % theuuid;
    nLines = db_curs.execute(qtext)

    if nLines == 1: # it may be a valid request
        dbvals = db_curs.fetchone()
        if (dbvals[3] == "2") | (dbvals[3] == "3"): # the stream is already connected!
            print "Someone tried to reconnect to an aleady warm/hot stream!"
            return "N"
        else:
            replystr = "Y %s " % (dbvals[2])
            # Update with new remote ID and set state to WARM
            db_curs.execute("UPDATE datastreams SET rx_uuid=%s, status=%i WHERE tx_uuid = %s;" %(remote_uuid, STREAMSTATE['WARM'], theuuid) )
            # Announce the change of state
            announceMsg( "A key=%s state=%i" % (dbvals[2], STREAMSTATE['WARM']) )
            return replystr
    else:
        drop = db_curs.fetchall()
        return "N" # eff them

#----------
def instream_updateExisting(tx_uuid, newfreq, pauseit):

    nLines = db_curs.execute("SELECT * FROM datastreams WHERE tx_uuid = '%s';" % (tx_uuid))

    dbvals = db_curs.fetchone();
    ann_string = "A key=%s " % (dbvals[2])

    if newfreq >= 0.0:
        setFreq = newfreq
        ann_string = ann_string + "freq=%f " % newfreq
    else:
        setFreq = float(dbvals[5])

    if pauseit >= 0:
        if pauseit > 0:
            newState = STREAMSTATE['WARM']
            ann_string = ann_string + "state=%i " % newState
        else:
            newState = STREAMSTATE['HOT']
            ann_string = ann_string + "state=%i " % newState

    nLines = db_curs.execute("UPDATE datastreams SET freq=%f, state=%i WHERE tx_uuid = '%s';" % (setFreq, newState, tx_uuid))

    if nLines == 1:
        reply = "Y %i" % (newState)
        announceMsg(ann_string)
    else:
        reply = "N"

    return reply

#----------
def instream_disconnect(remote_key, action):
    nLines = db_curs.execute("SELECT routekey FROM datastreams WHERE tx_uuid = '%s';" % (remote_key))
    dbvals = db_curs.fetchone()

    if nLines == 1:
	if action == 1: # cold shutdown
            db_curs.execute("UPDATE datastreams SET status = 1 WHERE tx_uuid='%s';" % (remote_key) )
	    announceMsg( "A key=%s state=1" % (dbvals[0]) )
	if action == 0: # kill
            db_curs.execute("DELETE FROM datastreams WHERE tx_uuid = '%s';" % (remote_key))
            announceMsg( "D %s" % (dbvals[0]) )
        return "Y"
    else:
        return "N"


# Better that this be hardcoded here than throughout the code
DBHOST = os.environ['STREAMBOSS_DBHOST']
DBUSER = os.environ['STREAMBOSS_DBUSER']
DBPASS = os.environ['STREAMBOSS_DBPASS']
DBDB = os.environ['STREAMBOSS_DBNAME']

RMQHOST = os.environ['STREAMBOSS_RABBITMQ_HOST']

E_STREAM_NAMETAKEN = -100;

STREAMSTATE = {'COLD' : 0, 'COOL' : 1, 'WARM' : 2, 'HOT' : 3}

# The central driver code:
# Open connections to the database server
db_connection = sql.connect(DBHOST, DBUSER, DBPASS, DBDB)
db_connection.autocommit(True)
db_curs = db_connection.cursor()

# Open connections to the message server
rmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=RMQHOST))
channel = rmq_connection.channel()

# Make sure the stream exchanges exist
channel.exchange_declare(exchange='streams', type='direct', durable=True)
channel.exchange_declare(exchange='stream_announce', type='fanout', durable=True)

# declare the reg_stream queue and bind it to the default exchange
registerQueue = channel.queue_declare(queue='reg_stream', durable=True)
#channel.queue_bind(exchange='', queue=registerQueue.method.queue, routing_key='reg_stream')

channel.basic_consume(handle_streamreq, no_ack=True, queue=registerQueue.method.queue)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

rmq_connection.close()
