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
import time
import string
import pipes
import MySQLdb as sql

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
# So I'm naming things after mesons it seems.
delta = Key(bukkit)

################################################################################
def keyval_get(key, chunk):
    x = string.split(chunk, " ")
    for N in range(0, len(x)):
        y = string.split(x[N], "=")
        if y[0] == key:
            return y[1]
    return 999;

# This will be our callback to handle incoming messages
def rerunRegressionPlot(plotfile):
        DBHOST = os.environ['STREAMBOSS_DBHOST']
        DBUSER = os.environ['STREAMBOSS_DBUSER']
        DBPASS = os.environ['STREAMBOSS_DBPASS']

        db = sql.connect(DBHOST, DBUSER, DBPASS, 'Archive')
        curs = db.cursor()

        Nchloro = curs.execute("SELECT * FROM chloro_meas ORDER BY unixtime")
        cf = open("chlorofile.txt","w")
        for q in range(0, Nchloro):
            x = curs.fetchone()
            cf.write("%s %s\n" % (x[3], .01*float(x[4]), ))
        cf.flush()
        cf.close()

        Nndvis  = curs.execute("SELECT * FROM ndvi_meas ORDER BY unixtime")
        nf = open("ndvifile.txt","w")
        for q in range(0, Nndvis):
            x = curs.fetchone()
            nf.write("%s %s\n" % (x[3], x[4], ))
        nf.flush()
        nf.close()

        P = pipes.Template()
        P.append('/usr/bin/gnuplot','--')
        drive = P.open('gnuoutput.txt','w')

        drive.write('set terminal postscript landscape enhanced color linewidth 1.5 size 10in,7.5in\n')
        drive.write('set output \"new_regress.ps\"\n')
        drive.write('set xlabel \"Unix time since epoch\"\nset ylabel \"Measured values\"\n')
        drive.write('plot   \'chlorofile.txt\' using 1:2 title \'Chlorophyll measurements * .01\'\n')
        drive.write('replot \'ndvifile.txt\'   using 1:2 w lp title \'Space-averaged NDVI\'\n')
        drive.flush()
        drive.close()
        
        delta.key = 'regress_latest_ps';
        delta.set_contents_from_filename('new_regress.ps')

        cloud.sendStreamItem("NEW_PLOT regress_latest_ps")

def markForReplot(method, props, body):
    X = string.split(body, ' ')
    if(X[0] == "NEW"):
        # FIXME: I'm ignoring the db/table arguments that the ndvi sampler and the chlorophyll uploader emit
        # FIXME: corners, remember? they're being cut.
        return 1
    return 0


################################################################################
# Time to get a Streamboss connection and boost this turkey off the ground...
# Let's do this,
# WRAAAAAAAAAAAAAAAAAAAGGGGGGGHHHHH!

cloud = RabbitAdapter.CloudAdapter()
# FIXME: No security here. That should be unbroked at some point.
cloud.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

#cloud.setRxCallback(cubestreamHandler)
cloud.streamAnnounce('regression_pix', 'regress_compare')
cloud.waitForPikaThread()

# Get update messages from both the cube processor and the chlorophyll cloud-side receiver 
cloud.streamSubscribe("ndvi_samples")
cloud.streamSubscribe("chloro_upload")

replotNext = 0
sleptfor = 0

# Go forever...
while True:
    while len(cloud.receiveFifo) > 0:
        x = cloud.receiveFifo.popleft()
        replotNext = replotNext | markForReplot(x[0], x[1], x[2])

    # Check if we should go die
    f0 = open('/root/checklife','r')
    g0 = f0.readline()
    f0.close()
    if g0[0] != "1":
        break

    # Sleep and mark it down
    time.sleep(.1)
    sleptfor += .1

    # If we've received word of new data and it's been at least 5 seconds, replot
    if (replotNext == 1) & (sleptfor > 5):
        rerunRegressionPlot("regress_latest.ps")
        sleptfor = 0
        replotNext = 0
        cloud.sendStreamItem("NEW PLOT")


# Disconnect & kill everything
cloud.streamUnsubscribe('forest_chloro_readings');

cloud.streamShutdown(0)
cloud.waitForPikaThread()

cloud.disconnectFromExchange()

quit()

