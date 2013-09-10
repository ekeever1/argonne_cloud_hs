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
pion = Key(bukkit)

################################################################################
# Set up the bits that handle the incoming files and parse the hs_transmit_dir messages
# Let's teleport some state up into this hizzy...



def keyval_get(key, chunk):
    x = string.split(chunk, " ")
    for N in range(0, len(x)):
        y = string.split(x[N], "=")
        if y[0] == key:
            return y[1]

def ndvi_handler(method, props, body):
    X = string.split(body)
    if X[0] != "CUBE_PROCESSED":
        return

    # Download the key_ndvi_exr
    keyprefix=X[1]+"_"+X[2]
    pion.key = keyprefix+"_ndviexr"
    pion.get_contents_to_filename("NDVI.exr")

    # FIXME: Ugly hack, I hardcoded the polygon file to use into this VM image.
    # FIXME: COrners are definitely being cut at this point.
    # Pass it to the ndvi sampler
    call(['./ndviSampler','polyfile.csv','NDVI.exr']) 

    # Store [time, ndvi_mean] in the sql database
    DBHOST = os.environ['STREAMBOSS_DBHOST']
    DBUSER = os.environ['STREAMBOSS_DBUSER']
    DBPASS = os.environ['STREAMBOSS_DBPASS']

    db = sql.connect(DBHOST, DBUSER, DBPASS, 'Archive')
    curs = db.cursor()
    #FIXME: HACK hack hack hack ugly hack, hardcoding the space coordinate of origin to the MCS 6th floor north balcony site
    curs.execute("INSERT INTO ndvi_meas (latitude, longitude, altitude, unixtime, ndvi) VALUES (%f, %f, %f, %s, %s)\n" % (41.7184833, -87.983466, 270, cubetime, cubemean) )
    curs.commit()
    cloud.sendStreamItem("NEW db=Archive table=ndvi_meas")

################################################################################
# Time to get a Streamboss connection and boost this turkey off the ground...
# Let's do this,
# WRAAAAAAAAAAAAAAAAAAAGGGGGGGHHHHH!

cloud = RabbitAdapter.CloudAdapter()
# FIXME: No security here. That should be unbroked at some point.
cloud.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

#cloud.setRxCallback(cubestreamHandler)
cloud.streamAnnounce('ndvi_samples', 'ndvi_analyzer')
cloud.waitForPikaThread()

cloud.streamSubscribe('hyperspec_calib')

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
cloud.streamUnsubscribe('hyperspec_upload1');

cloud.streamShutdown(0)
cloud.waitForPikaThread()

cloud.disconnectFromExchange()

quit()

