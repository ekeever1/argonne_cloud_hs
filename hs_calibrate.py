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
import pipes

import RabbitAdapter
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
# Prepare a pipe to hscal
t = pipes.Template()
t.append('hscal -nogui','--')
f = t.open('caloutput','w')

f.write('cube load data=democube.cube\n')
f.write('calib forcube\n')
f.write('calib load mode=U file=SK_Xenoplan35.Radcal\n')

def keyval_get(key, chunk):
    x = string.split(chunk, " ")
    for N in range(0, len(x)):
        y = string.split(x[N], "=")
        if y[0] == key:
            return y[1]

# This will be our callback to handle incoming messages
def calibrationHandler(method, props, body):
    # We look for messages in the form of 
    # CUBE_READY sequence=8d152844af6e40db cubenumber=1 xcal=10 ycal=20 rcal=3 time=1376847057

    M = string.split(body, ' ')
    
    if (M[0] == "CUBE_READY"):
        print "Got CUBE_READY!"
        # This is a cube ready to be processed!
        # Grab all the pretty instructions from the message
        seqKey   = keyval_get("sequence", body)
        cubenum  = keyval_get("cubenumber", body)
        xcal     = keyval_get("xcal", body)
	ycal     = keyval_get("yval", body)
	rcal     = keyval_get("rcal", body)
	cubetime = keyval_get("time", body)

        # Download the HS data to local storage since hscal doesn't natively speak Cloud
        # These are stored in (key)_calprofile and (key)_rawcube_SequenceNumber
        kaon.key = "%s_calprofile" % (seqKey, )
        kaon.get_contents_to_filename("calprofile.csv")
        kaon.key = "%s_rawcube_%s" % (seqKey, cubenum)
        kaon.get_contents_to_filename("thedata.cube")
        print "got hypercube"

        # Begin pouring commands down the pipe to hscal
	f.write("calib load mode=T file=calprofile.csv comments=0\n")
        f.write("cube load data=thedata.cube\n")
        f.write("cube usecal N=0\n")
        f.write("cube calset radio=1 spectral=1\n")
        f.write("calib referU point=<%s,%s> radius=%s checkshade=1\n" % (xcal, ycal, rcal))
        f.write("planes new R=55 G=34 B=13\n")
        f.write("img rgb file=thecube_rgb.tiff\n")
        f.write("img hamada prefix=thecube fmt=tiff\n")
        f.write("cube save file=cubeout.nc\n")
        f.flush() # Let it digest this for a bit...

        print "Sent commands to hscal"

        time.sleep(10); # Ample time for digestion to complete

        # Okay images are coming out
        kaon.key = "%s_eyetiff" % (seqKey, )
        kaon.set_contents_from_filename("thecube_rgb.tiff")
        kaon.key = "%s_ndvitiff" % (seqKey, )
        kaon.set_contents_from_filename("thecube_vegindex.tiff");
       # Oy, this is a big one...
        kaon.key = "%s_calibbed_%s.nc" % (seqKey, cubenum, )
        kaon.set_contents_from_filename("cubeout.nc")
        
        cloud.sendStreamItem("CUBE_PROCESSED %s %s" % (seqKey, cubenum) )

       
################################################################################
# Time to get a Streamboss connection and boost this turkey off the ground...
# Let's do this,
# WRAAAAAAAAAAAAAAAAAAAGGGGGGGHHHHH!

cloud = RabbitAdapter.CloudAdapter()
# FIXME: No security here. That should be unbroked at some point.
cloud.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

#cloud.setRxCallback(cubestreamHandler)
cloud.streamAnnounce('hyperspec_calib', 'hyperspec_calib')
cloud.waitForPikaThread()

cloud.streamSubscribe('hyperspec_raw')

# We have our callback set so there's nothing for this thread to do but wait for an exit signal...
while True:
    while len(cloud.receiveFifo) > 0:
        x = cloud.receiveFifo.popleft()
        calibrationHandler(x[0], x[1], x[2])
    f0 = open('/root/pydrive/checklife','r')
    g0 = f0.readline()
    f0.close()
    if g0[0] != "1":
        break
    time.sleep(.1)

# Disconnect & kill everything
cloud.streamUnsubscribe('hyperspec_raw');

cloud.streamShutdown(0)
cloud.waitForPikaThread()

cloud.disconnectFromExchange()

quit()

