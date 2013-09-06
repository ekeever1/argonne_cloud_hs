#!/usr/bin/env python

import io
import os
import base64
import uuid
import string

class chunkSender(object):
    fileObject = []
    filePosition = 0
    fileName = ""

    chunksize = -1
    totalsize = 0
    totalsent = 0
    
    keynum = 0

    # Opens the file and gets ready to go
    def __init__(self, filepath, chunksize):
        self.fileObject = io.open(filepath, 'rb')
        self.filePosition = 0
        self.fileName = filepath

        self.chunksize = chunksize
        self.totalsize = os.path.getsize(filepath)
        self.totalsent = 0

        self.keynum = uuid.uuid4().int / 36893488147419103232

    # Sends the chunker "HERE I GO" message
    def getBeginMessage(self):
        # Begin message: "CHUNKER key totalsize blocksize filename"
        return "CHUNKER %i %i %i %s" % (self.keynum, self.totalsize, self.chunksize, self.fileName)

    # Sends a chunk of encoded data
    def getNextChunk(self):
        # If finished, send "CHUNKER FINISHED key"
        if(self.totalsent == self.totalsize):
            return "CHUNKER %i FINISHED" % (self.keynum, )

        # Read a block, send:
	# CHUNKER key base64d_block
        dblock = self.fileObject.read(self.chunksize)
        self.totalsent = self.totalsent + len(dblock)

        return "CHUNKER %i " % (self.keynum,) + base64.b64encode(dblock)

    def checkFinished(self):
        if(self.totalsent == self.totalsize):
            return True
        else:
            return False

class chunkReceiver(object):
    nextFilename = ""
    rxInProgress = 0

    totalsize = 0
    chunksize = 0
    totalgot = 0

    keynum = 0

    rxDatablock = []
    rxFileObject = []

    def __init__(self):
        self.nextFilename = "tempfilename"

    def handleMsg(self, body):
        first = string.split(body, " ", 1)
        if first[0] == "CHUNKER":
            if self.rxInProgress == 1:
                return self.receiveChunk(body)
            else:
                self.receiveBegin(body, self.nextFilename)
                return 0
        else:
            return -1

    def receiveBegin(self, starterMessage, dumpfile):
        bits = string.split(starterMessage)

        if bits[0] != "CHUNKER":
	    return False

        self.rxInProgress = 1
        self.keynum = long(bits[1])
        self.totalsize = long(bits[2])
        self.totalgot  = 0
        self.chunksize = long(bits[3])
        
        if (dumpfile != -1):
            self.rxFileObject = io.open(dumpfile, 'wb')
	else:
	    self.rxFileObject = -1

        return

    def receiveChunk(self, chunk):
        X = string.split(chunk)
        if (X[0] != "CHUNKER"):
            return -1

        if (long(X[1]) != self.keynum):
	    return -1

        if (self.totalgot >= self.totalsize):
            if (X[2] != "FINISHED"):
                return -1
            else:
                if (self.rxFileObject != -1):
                        self.rxFileObject.close()
                self.rxInProgress = 0
                return 1
            
        nublock = base64.b64decode(X[2])

        if(self.rxFileObject == -1):
	    self.rxDatablock.append(nublock)
	else:
	    self.rxFileObject.write(nublock)

        self.totalgot = self.totalgot + len(nublock)

        return 0 

    def isFinished(self):
        if self.totalgot == self.totalsize:
	    return True
        else:
	    return False


