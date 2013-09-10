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
def keyval_get(key, chunk):
    x = string.split(chunk, " ")
    for N in range(0, len(x)):
        y = string.split(x[N], "=")
        if y[0] == key:
            return y[1]
    return 999;

# This will be our callback to handle incoming messages
def rerunRegressionPlot(method, props, body):
    X = string.split(body, ' ')
    if (X[0] == "NEW"):
        DBHOST = os.environ['STREAMBOSS_DBHOST']
        DBUSER = os.environ['STREAMBOSS_DBUSER']
        DBPASS = os.environ['STREAMBOSS_DBPASS']

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

# Get update messages from both the cube processor and the chlorophyll cloud-side receiver 
cloud.streamSubscribe("ndvi_samples")
cloud.streamSubscribe("chloro_upload")

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

