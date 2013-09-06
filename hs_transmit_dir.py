#!/usr/bin/env python

import os
import RabbitAdapter
import FileChunker   as FC
import uuid
import string
import time

# List the HS .cube and .dark files in the current directory
things = os.listdir('.')

sender = RabbitAdapter.SensorAdapter()

sender.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

streamkeyfile = []

try:
    streamkeyfile = open('./cube_streamer_key.txt','r')
    streamKey = streamkeyfile.readline() # Try to get the original token from its totally secure plaintext file
    sender.streamReconnect(streamKey) # Attempt reconnect
    sender.waitForPikaThread()
except:
    sender.streamRequestNew("hyperspec_upload1", "hsmonitor", 0.0)
    sender.waitForPikaThread() # Connect to new stream

    streamkeyfile = open('./cube_streamer_key.txt','w')
    streamkeyfile.write(sender.myTxid.__str__())
    streamkeyfile.close()
    
# Assume we have a stream open now. We should. If we don't something's boned.

try:
    calinfo = open('cal_instruct.txt','r')
except: # Oh balls
    print "You need to have a cal_instruct.txt file so I can tell the cloud processor what the hell to do dude."
    print "Put in it the line\nx=# y=# radius=# panelprofile=FILENAME"
    sender.streamShutdown(1)
    sender.waitForPikaThread()
    sender.disconnectFromExchange()
    sender.waitForPikaThread()
    quit()

def parseCalInstruct(key, line):
    y = string.split(line,' ')
    for N in range(0,len(y)):
        z = string.split(y[N],'=')
        if z[0] == key:
            return string.strip(z[len(z)-1])
    return "FAIL"

theline = calinfo.readline()
calinfo.close()

calx = parseCalInstruct("x", theline)
caly = parseCalInstruct("y", theline)
calR = parseCalInstruct("radius", theline)
calPanelFile = parseCalInstruct("panelprofile", theline)

if (calx == "FAIL") | (caly == "FAIL") | (calR == "FAIL") | (calPanelFile == "FAIL"):
    print "cal_instruct.txt must contain\nx=# y=# radius=# panelprofile=FILENAME\non the first line. aborting."
    sender.streamShutdown(1)
    sender.waitForPikaThread()
    sender.disconnectFromExchange()
    sender.waitForPikaThread()
    quit()

def iscube(filename):
    if len(filename) < 6:
        return False
    if filename[-4:] == "cube":
        return True
    else:
        return False

def isdark(filename):
    if len(filename) < 6:
        return False
    if filename[-4:] == "dark":
        return True
    else:
        return False

# This assumes that all hypercubes in the current directory share calibration information given in cal_instruct.txt

# Get a complete list of hypercubes stored here and any associated .dark files
cubelist = []
darklist = []
for N in range(0, len(things)):
    y = things[N]
    if iscube(y):
        cubelist.append(y)
    if isdark(y):
        darklist.append(y)

cubetimes = []
for N in range(0, len(cubelist)):
    y = string.split(cubelist[N],'_')
    cubetimes.append(time.mktime(time.strptime(y[3]+"_"+y[4], "%d-%m-%Y_%H.%M.%S.cube")))

darkdates = []
for N in range(0, len(darklist)):
    y = string.split(darklist[N],'_')
    darktimes.append(time.mktime(time.strptime(y[3]+"_"+y[4], "%d-%m-%Y_%H.%M.%S.cube")))

ND = len(darklist)

# FIXME: This needs to check if the required radiometric calibration file has been uploaded
# FIXME: I am in desperate straits and will simply hardcode that in.

# This initiates a sequence of uploaded hypercubes;
sequenceKey = uuid.uuid4().hex[0:16]

def fileSender(filename):
    # Transmit start message
    chunker = FC.chunkSender(filename, 49152)
    sender.sendStreamItem(chunker.getBeginMessage())
    # Push file in 48K chunks
    while chunker.totalsent < chunker.totalsize:
        sender.sendStreamItem(chunker.getNextChunk())
    # Transmit end message
    sender.sendStreamItem(chunker.getNextChunk())
   

# We will follow a simpleminded format of sending what we need:
# SEQUENCE_START key
# CALPANEL
# <upload calibration panel profile>, small file
# SEQUENCE CUBES_ONLY
# SEQUENCE CUBE xcal=XX ycal=YY radius=RR date=TT
# <upload cube 0>
# SEQUENCE CUBE xcal=XX ycal=YY radius=RR date=TT
# <upload cube 1>
# ...
# SEQUENCE END

sender.sendStreamItem("SEQUENCE_START %s" % (sequenceKey, ) )
sender.sendStreamItem("CALPANEL %s" % (sequenceKey, ) )
fileSender(calPanelFile)

# We're gonna wing it, forget the dark frames for now...
#if ND == 0: # No dark frames; Send instructions to calibrate with no dark frame subtraction
if True:
    sender.sendStreamItem("CUBES_ONLY %s" % (sequenceKey, ) )
    for N in range(0, len(cubelist)):
        sender.sendStreamItem("CUBE %s xcal=%s ycal=%s rcal=%s time=%i" % (sequenceKey, calx, caly, calR, cubetimes[N], ))
        fileSender(cubelist[N])
    
elif ND == 1:# One dark frame; Send instruction to calibrate with subtracting it
    stuff
else: # Multiple dark frames; Send cloest dark frame then cube, one by one
    stuff

sender.sendStreamItem("SEQUENCE_END %s" % (sequenceKey, ) )

sender.streamShutdown(1)
sender.waitForPikaThread()
sender.disconnectFromExchange()

quit()

