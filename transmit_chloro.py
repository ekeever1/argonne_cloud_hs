#!/usr/bin/env python

import os
import RabbitAdapter
import uuid
import string
import time
import sys
import MySQLdb as sql

# GET CHLOROPHYLL MEASUREMENT FILE WE ARE UPLOADING
print "Input chlorophyll data file to upload: ",
cf = sys.stdin.readline()

try:
    chloro = open(cf,'r')
except:
    print "Couldn't open "+cf+" for read. Try again."
    quit()

# GET STREAMBOSS CONNECTION
sender = RabbitAdapter.SensorAdapter()
sender.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])
streamkeyfile = []

try:
    streamkeyfile = open('._chloro_key.txt','r')
    streamKey = streamkeyfile.readline() # Try to get the original token from its totally secure plaintext file
    sender.streamReconnect(streamKey) # Attempt reconnect
    sender.waitForPikaThread()
except:
    sender.streamRequestNew("forest_chloro_readings", "chloro_sender", 0.0)
    sender.waitForPikaThread() # Connect to new stream

    streamkeyfile = open('./chloro_key.txt','w')
    streamkeyfile.write(sender.myTxid.__str__())
    streamkeyfile.close()

# GET SQL CONNECTION


def keyvalue_get(key, line, defaultval):
    y = string.split(line,' ')
    for N in range(0,len(y)):
        z = string.split(y[N],'=')
        if z[0] == key:
            return string.strip(z[len(z)-1])
    return defaultval

# Assume we have a stream open now. We should. If we don't something's boned.

# Initialize the values we are going to upload
time  = 0
lat   = 999
long  = 999
alt   = 999;
value = -1

for X in chloro:
    S = split(X,' ')
    if (strip(S[0]) == "position"):
        v = keyvalue_get("lat", X, 999)
        if(v == 999):
            continue
        lat = float(v)
        v = keyvalue_get("long", X, 999)
        if(v == 999):
            continue
        long = float(v)
        v = keyvalue_get("alt", X, 999)
        if(v == 999):
            continue
        alt = float(v)

    if (strip(S[0]) == "time"):
        time = time.mktime(time.strptime(string.strip(S[1]), "%Y-%m-%d_%H:%M:%S"))

    if (strip(S[0]) == "value"): # This is the money!
        value = float(string.strip(S[1]))
        sender.sendStreamItem("SAMPLE latitude=%s longitude=%s alt=%s time=%s value=%s" % (lat, long, alt, time, value)

sender.streamShutdown(1)
sender.waitForPikaThread()
sender.disconnectFromExchange()

quit()

