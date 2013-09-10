#!/usr/bin/python
#Original Author: Nick Bond
#Purpose: This script allows the user to connect to an S3 cloud storage source
#   and then create a new bucket and key. After that the user is able to
#   upload a file and append the bucket with the new key that houses their
#   file. A MySQL dump is used for this example
#
# Revised: Erik Keever
# This forms the frontend of our hyperspectral upload system;
# It immediately faces the uploader stream
# 
# This currently uses a brain-damaged file uploader scheme to stuff entire 100MB
# hyperspectral data files through the stream system. This is partly as a proof-
# of concept for an eventually less-retarded backup.
#
# The properly written stream source should attempt to connect to S3 itself and
# just push the bucket/key over the stream system, only resorting to machine gunning
# 1886 messages per hypercube through Rabbit if unable to reach S3.
# 
# At any rate, this frontend will (one way or another, damnit!) get the hypercube
# uploaded into S3, and emit [calkey, cubekey, panelkey, calx, caly, calR] messages
# onto its stream

import os

import boto 
import sys
import uuid
import boto.s3.connection
from boto.s3.key import Key

import RabbitAdapter
import FileChunker
import time
import string

################################################################################
# This is the magic cribbed unaltered from Nick's bucket_getter.py code
# I am lost if this doesn't work
# Get an S3 connection
access_key_id = os.environ['AWS_ACCESS_KEY_ID']
secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
##Connecting to cloud storage##
conn=boto.s3.connection.S3Connection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, #Connecting to Cumulus
is_secure=False, port=8888, host='svc.uc.futuregrid.org',
debug=0, https_connection_factory=None, calling_format = boto.s3.connection.OrdinaryCallingFormat())

# FIXME: This is hardcoded. That's probably bad...
bukkit = conn.get_bucket('keever_test')
# So I'm naming things after elementary color-interacting particles it seems.
kaon = Key(bukkit)

################################################################################
# Set up the bits that handle the incoming files and parse the hs_transmit_dir messages
# Let's teleport some state up into this hizzy...
cubeSequenceKey = -1
currentAction   = 0 # 0 = nothing, 1 = getting cal panel, 2 = getting cube
cubesInSequence = 0
sequenceInfo    = ""

# Prepare a chunker to receive bulk transfers
chunker = FileChunker.chunkReceiver()

def keyval_get(key, chunk):
    x = string.split(chunk, " ")
    for N in range(0, len(x)):
        y = string.split(x[N], "=")
        if y[0] == key:
            return y[1]
    return 999;

# This will be our callback to handle incoming messages
def chloroSampleHandler(method, props, body):
    # I don't give a flaming shit any more, JUST WORK YOU FUGGING WRETCHED TURDBOMB
    global cubeSequenceKey
    global currentAction
    global cubesInSequence
    global sequenceInfo

    X = string.split(body, ' ')
    if (X[0] == "SAMPLE"):
        DBHOST = os.environ['STREAMBOSS_DBHOST']
        DBUSER = os.environ['STREAMBOSS_DBUSER']
        DBPASS = os.environ['STREAMBOSS_DBPASS']

        lat   = keyval_get("latitude", body)
        long  = keyval_get("longitude", body)
        alt   = keyval_get("alt", body)
        unixt = keyval_get("time", body)
        chval = keyval_get("value", body)

        db = sql.connect(DBHOST, DBUSER, DBPASS, 'Archive')
        curs = db.cursor()
        curs.execute("INSERT INTO chloro_meas(latitude, longitude, altitude, unixtime, chloro) values (%s, %s, %s, %s, %s)\n" % (lat, long, alt, unixt, chval) );
        cloud.sendStreamItem("NEW db=Archive table=chloro_meas")



################################################################################
# Time to get a Streamboss connection and boost this turkey off the ground...
# Let's do this,
# WRAAAAAAAAAAAAAAAAAAAGGGGGGGHHHHH!

cloud = RabbitAdapter.CloudAdapter()
# FIXME: No security here. That should be unbroked at some point.
cloud.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

#cloud.setRxCallback(cubestreamHandler)
cloud.streamAnnounce('chloro_upload', 'chlorophyll_upload')
cloud.waitForPikaThread()

cloud.streamSubscribe("forest_chloro_readings")

# We have our callback set so there's nothing for this thread to do but wait for an exit signal...
while True:
    while len(cloud.receiveFifo) > 0:
        x = cloud.receiveFifo.popleft()
        cubestreamHandler(x[0], x[1], x[2])
    f0 = open('/root/checklife','r')
    g0 = f0.readline()
    f0.close()
    if g0[0] != "1":
        break
    time.sleep(.1)

# Disconnect & kill everything
cloud.streamUnsubscribe('forest_chloro_readings');

cloud.streamShutdown(0)
cloud.waitForPikaThread()

cloud.disconnectFromExchange()

quit()

