#!/usr/bin/env python

import pika
import uuid
import time
import threading
import atexit
from collections import deque

# For simlest-working-example, see console_stream.py
#
# There are four stream status values (much as there are course only four lights):
# 3 = HOT:   VM on, connected, actively sampling
# 2 = WARM:  VM on, connected, not sampling
# 1 = COOL:  To server, (VM on,  no connection); To client, (connected, no stream setup)
# 0 = COLD:  VM off, no connection
# 
# Events: new, sample, state_request, disconnect, reconnect
# Client adaptor state transition map:
#      | new        sample     pause        d/c         r/c         r/c_srv
#------+------------------------
# HOT  | HOT	    HOT        as_rq        COLD        NOT_P       NOT_P
# WARM | NOT_P      HOT        as_rq        COLD        NOT_P       NOT_P
# COOL | HOT        NOT_P      NOT_P        COLD        WARM        NOT_P
# COLD | NOT_P      NOT_P      NOT_P        NOT_P       WARM        COOL
#
# RMQ server adaptor state transition map:
#      | new        sample     state_r      d/c        r/c        
#------+------------------------
# HOT  | HOT        HOT        as_rq        COOL       NOT_P
# WARM | NOT_P      HOT        as_rq        COOL       NOT_P
# COOL | NOT_P      NOT_P      NOT_P        NOT_P      WARM
# COLD | NOT_P      NOT_P      NOT_P        NOT_P      WARM

################################################################################
class SensorAdapter(object):
    '''Frontend python adaptor for Forest Project sensor streaming architecture'''
    statemap = {'COLD' : 0, 'COOL' : 1, 'WARM' : 2, 'HOT' : 3}
    conn         = [] # pika connection
    chan         = [] # pika channel
    streamState  = statemap['COLD'] # dead

    myTxid       = -1 # ID passed back to us; -1 = no stream exists: Proof that you are the creator of & have right to alter this stream
    myRxid       = uuid.uuid4().int / 36893488147419103232 # Acquired by central dispatch on start, used for callback ID
    myRoutekey   = [] # Our routing key for sending stream messages

    replyQueue   = [] # An exclusive queue for callback stuff
    rpc_wait     = 0
    rpc_expect   = 0  # 0 = nothing, 1 = new, 2 = reconnect, 3 = alter, 4 = delete
    
    shutdownRequested = False

    timeoutHandle = []
    clearToQuit   = 1

    def __init__(self):
        atexit.register(self.emergencyPikathreadExit)
 
    def connectToExchange(self, hostname, user, passwd):
        '''Attempts to plug adaptor into a supporting RMQ server'''
        try: # Attempt to get a connection
            creds = pika.PlainCredentials(user, passwd)
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, credentials=creds))
        except:
            print "conn failed"

        try:
            self.chan = self.conn.channel()
        except:
            print "channel failed"

        self.streamState = self.statemap['COOL']

        # Setup a reply callback handler
        self.replyQueue = self.chan.queue_declare(exclusive=True)
        self.chan.basic_consume(self.replyHandler, no_ack=True, queue=self.replyQueue.method.queue)

        self.timeoutHandle = self.conn.add_timeout(.2, self.checkForExit)

        # Drop the pika event loop into another thread
        threading.Thread(target=self.chan.start_consuming, args=(), name="pikathread").start()
        clearToQuit = 0

        return True

    def disconnectFromExchange(self):
        '''Client completely disconnects from Rabbit server'''
        self.rpc_wait = 1;
        self.shutdownRequested = True;
	while (self.rpc_wait == 1) & (self.clearToQuit == 0):
            pass

    def streamRequestNew(self, streamName, processor, freq=0.0):
        '''Client attempts to open a new stream, named streamName, at "freq" samples/second'''
	if (self.streamState == self.statemap['COLD']):
	    print "Error requesting stream: Not connected."
	    return False
        if (self.myTxid > 0):
	    print "Error requesting stream: Must close existing stream first."
            return False        

        # Ask for a new stream channel
        requestString = "N key=%s freq=%g process=%s" % (streamName, freq, processor)
        self.rpc_expect = 1
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='',routing_key='reg_stream', properties=pika.BasicProperties(reply_to=self.replyQueue.method.queue, correlation_id=self.myRxid.__str__()), body=requestString)
        return

    def streamReconnect(self, streamUUID):
        '''Try to re-establish a stream that was previously closed'''
        if (self.streamState == self.statemap['COLD']):
            print "Error requesting stream: Not connected."
            return False
        if (self.myTxid > 0): # Fail if we already have a stream
	    print "Error reconnecting: Must close existing stream first"
            return False

        requestString = "R %s" % (streamUUID)
        self.myTxid = long(streamUUID)
        self.rpc_expect = 2
	self.rpc_wait = 1
        self.chan.basic_publish(exchange='', routing_key='reg_stream',properties=pika.BasicProperties(reply_to=self.replyQueue.method.queue, correlation_id=self.myRxid.__str__()),body=requestString)
        return

    def waitForPikaThread(self):
        while self.rpc_wait == 1:
            time.sleep(.05)

    def streamAlter(self, pauseme = -1, newfreq = -1.0):
        '''Allows client to change expected samplerate or indicate a pause'''
        if (self.myTxid < 0):
            return False

        reqStr = "A %i " % (self.myTxid)
        if (pauseme >= 0):
            reqStr = reqStr + "pauseme=%i " % (pauseme)
        if (newfreq >= 0.0):
            reqStr = reqStr + "freq=%f " % (newfreq)

        self.rpc_expect = 3
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='', routing_key='reg_stream',properties=pika.BasicProperties(reply_to=self.replyQueue.method.queue, correlation_id=self.myRxid.__str__()),body=reqStr)
        return

    def streamShutdown(self, intoState):
        '''Client puts an open stream into the COOL state'''
        if (self.streamState > 1) & (self.myTxid > 0):
            actions = { 0 : 'kill', 1 : 'cold' }
            requestString = "D %i action=%s" % (self.myTxid, actions[intoState])
            self.rpc_expect = 4
            self.rpc_wait = 1
            self.chan.basic_publish(exchange='', routing_key='reg_stream',
                properties=pika.BasicProperties(reply_to=self.replyQueue.method.queue, correlation_id=self.myRxid.__str__()),
                body=requestString)

    def sendStreamItem(self, theString):
        if (self.myTxid > 0) & (self.shutdownRequested == False):
                self.chan.basic_publish(exchange='streams',routing_key=self.myRoutekey,body=theString)
        else:
                print "You can't send a message, you have no valid tx id or shutdoQueueprogress."

#---------- Below this shouldn't be user-called
    def emergencyPikathreadExit(self):
        # Registered via atexit to take down the pika thread in event of random error or quit
	self.shutdownRequested = True;

    def checkForExit(self):
        if self.shutdownRequested == True:
            self.conn.remove_timeout(self.timeoutHandle)
            self.chan.stop_consuming()
            self.conn.close()
            self.chan = []
            self.conn = []
            self.streamState = self.statemap['COLD']
            self.clearToQuit = 1
	    self.rpc_wait = 0
            quit()
        else:
            self.timeoutHandle = self.conn.add_timeout(.2, self.checkForExit)

    def replyHandler(self, ch, method, props, body):
        if(props.correlation_id != self.myRxid.__str__()):
            return
        if self.rpc_expect == 0:
            print "Unexpected message in my private queue: "+body
            return
        if self.rpc_expect == 1:
            self.cbNewStream(ch, method, props, body)
            return
        if self.rpc_expect == 2:
            self.cbReconnectStream(ch, method, props, body)
            return
        if self.rpc_expect == 3:
            self.cbAlterStream(ch, method, props, body)
            return
        if self.rpc_expect == 4:
            self.cbShutdownStream(ch, method, props, body)
            return

    def cbNewStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                P = body.split(); # ['Y', 'uuid_hex', 'routekey']
                self.myTxid = long(P[1], 10)
                self.myRoutekey = P[2]
                self.streamState = self.statemap['WARM']
        else:
                self.myTxid = -1
                self.streamState = self.statemap['COOL']

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbReconnectStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                P = body.split(); # ['Y', 'queuename']
                self.myRoutekey = P[1]
                self.streamState = self.statemap['WARM']
        else:
                self.myTxid = -1
                self.streamState = self.statemap['COOL']

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbAlterStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                parts = string.split(body);
                self.streamState = self.streamState[string.strip(parts[1])]

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbShutdownStream(self, ch, method, props, body):
        self.rpc_expect = 0
        self.rpc_wait = 0
        if (body[0] == 'Y'):
            self.myTxid       = -1 # no stream exists
            self.myRoutekey   = []
            self.streamState  = self.statemap['COOL']

################################################################################
class CloudAdapter(object):
    '''Cloud-side process adaptor for Forest Project sensor streaming architecture'''

    statemap = {'COLD' : 0, 'COOL' : 1, 'WARM' : 2, 'HOT' : 3}
    conn         = [] # pika connection
    chan         = [] # pika channel
    streamState  = statemap['COLD'] # dead

    # Low-level messaging stuff
    myTxid       = -1 # ID passed back to us; -1 = no stream exists: Proof that you are the creator of & have right to alter this stream
    myRxid       = uuid.uuid4().int / 36893488147419103232 # Acquired by central dispatch on start, used for callback ID
    myRoutekey   = [] # Our routing key for sending stream messages

    inputQueue   = [] # Input handler queue
    rpc_wait     = 0
    rpc_expect   = 0  # 0 = nothing, 1 = new, 2 = reconnect, 3 = alter, 4 = delete

    # Pika thread control stuff   
    shutdownRequested = False
    timeoutHandle = []
    clearToQuit   = 1

    # Higher level stuff
    processName = []
    receiveFifo = []
    receiveCallback = "INVALID"

    def __init__(self):
        atexit.register(self.emergencyPikathreadExit)
        self.receiveFifo = deque()
 
    def connectToExchange(self, hostname, user, passwd):
        '''Attempts to plug adaptor into a supporting RMQ server'''
        try: # Attempt to get a connection
            creds = pika.PlainCredentials(user, passwd)
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, credentials=creds))
        except:
            print "conn failed"

        try:
            self.chan = self.conn.channel()
        except:
            print "channel failed"

        self.streamState = self.statemap['COOL']

        # Setup a reply callback handler
        self.inputQueue = self.chan.queue_declare(exclusive=True)
        self.chan.basic_consume(self.replyHandler, no_ack=True, queue=self.inputQueue.method.queue)

        self.timeoutHandle = self.conn.add_timeout(.2, self.checkForExit)

        # Drop the pika event loop into another thread
        threading.Thread(target=self.chan.start_consuming, args=(), name="pikathread").start()
        clearToQuit = 0

        return True

    def disconnectFromExchange(self):
        '''Client completely disconnects from Rabbit server'''
        self.conn.close()
        self.chan = []
        self.conn = []
        self.streamState = self.statemap['COLD']

    def streamSubscribe(self, streamKey):
        '''Requests that a given routing key be sent to adapter's input queue'''
        self.chan.queue_bind(exchange='streams', queue=self.inputQueue.method.queue, routing_key=streamKey);

    def streamUnsubscribe(self, streamKey):
        '''Cancels a routing key binding'''
        self.chan.queue_unbind(exchange='streams', queue=self.inputQueue.method.queue, routing_key=streamKey);

    def setRxCallback(self, yourfunction):
        '''Convenience function; Pika thread will call function(method, properties, body) so user doesn't need to poll'''
        self.receiveCallback = yourfunction

    def streamAnnounce(self, streamName, functionName):
        '''Client attempts to open a new stream named streamName'''
        if (self.myTxid > 0):
            return False        

        # Ask for a new stream channel
        requestString = "N key=%s freq=%g process=%s" % (streamName, 0.0, functionName)
        self.rpc_expect = 1
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='',routing_key='reg_stream', properties=pika.BasicProperties(reply_to=self.inputQueue.method.queue, correlation_id=self.myRxid.__str__()), body=requestString)
        return

    def streamReconnect(self, streamUUID):
        '''Try to re-establish a stream that was previously closed'''
        if (self.myTxid > 0): # Fail if we already have a stream
            return False

        requestString = "R %s" % (streamUUID)
        self.myTxid = long(streamUUID)
        self.rpc_expect = 2
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='', routing_key='reg_stream',properties=pika.BasicProperties(reply_to=self.inputQueue.method.queue, correlation_id=self.myRxid.__str__()),body=requestString)
        return

    def waitForPikaThread(self):
        while self.rpc_wait == 1:
            time.sleep(.05)

    def streamAlter(self, pauseme = -1, newfreq = -1.0):
        '''Allows client to change expected samplerate or indicate a pause'''
        if (self.myTxid < 0):
            return False

        reqStr = "A %i " % (self.myTxid)
        if (pauseme >= 0):
            reqStr = reqStr + "pauseme=%i " % (pauseme)
        if (newfreq >= 0.0):
            reqStr = reqStr + "freq=%f " % (newfreq)

        self.rpc_expect = 3
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='', routing_key='reg_stream',properties=pika.BasicProperties(reply_to=self.inputQueue.method.queue, correlation_id=self.myRxid.__str__()),body=reqStr)
        return

    def streamShutdown(self, intoState):
        '''Client puts an open stream into the COOL state'''
        actions = { 0 : 'kill', 1 : 'cold' }
        requestString = "D %i action=%s" % (self.myTxid, actions[intoState])
        self.rpc_expect = 4
        self.rpc_wait = 1
        self.chan.basic_publish(exchange='', routing_key='reg_stream',
            properties=pika.BasicProperties(reply_to=self.inputQueue.method.queue, correlation_id=self.myRxid.__str__()),
            body=requestString)

    def sendStreamItem(self, theString):
        if (self.myTxid > 0) & (self.shutdownRequested == False):
                self.chan.basic_publish(exchange='streams',routing_key=self.myRoutekey,body=theString)
        else:
                print "You can't send a message, you have no valid tx id or shutdoQueueprogress."

#---------- Below this shouldn't be user-called
    def emergencyPikathreadExit(self):
        # Registered via atexit to take down the pika thread in event of random error or quit
        self.shutdownRequested = True;

    def checkForExit(self):
        if self.shutdownRequested == True:
            self.conn.remove_timeout(self.timeoutHandle)
            self.chan.stop_consuming()
            self.clearToQuit = 1
            self.rpc_wait = 0
            quit()
        else:
            self.timeoutHandle = self.conn.add_timeout(.2, self.checkForExit)

    def replyHandler(self, ch, method, props, body):
        if(props.correlation_id != self.myRxid.__str__()): # Default to normal inbound message
            if(self.receiveCallback == "INVALID"): # store in stack
                self.receiveFifo.append( (method, props, body) )
            else:
                while( len(self.receiveFifo) > 0 ): # Empty queue if msgs were waiting before callback was reg'd
                    m = self.receiveFifo.popleft()
                    self.recieveCallback( m[0], m[1], m[2] )

                self.receiveCallback(method, props, body)
            return

        # Otherwise we're probably waiting on an RPC
        if self.rpc_expect == 0:
            print "Unexpected message in my private queue: "+body
            return # Or maybe not
        if self.rpc_expect == 1:
            self.cbNewStream(ch, method, props, body)
            return
        if self.rpc_expect == 2:
            self.cbReconnectStream(ch, method, props, body)
            return
        if self.rpc_expect == 3:
            self.cbAlterStream(ch, method, props, body)
            return
        if self.rpc_expect == 4:
            self.cbShutdownStream(ch, method, props, body)
            return

    def cbNewStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                P = body.split(); # ['Y', 'uuid_hex', 'routekey']
                self.myTxid = long(P[1], 10)
                self.myRoutekey = P[2]
                self.streamState = self.statemap['WARM']
        else:
                self.myTxid = -1
                self.streamState = self.statemap['COOL']

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbReconnectStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                P = body.split(); # ['Y', 'queuename']
                self.myRoutekey = P[1]
                self.streamState = self.statemap['WARM']
        else:
                self.myTxid = -1
                self.streamState = self.statemap['COOL']

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbAlterStream(self, ch, method, props, body):
        if (body[0] == 'Y'):
                parts = string.split(body);
                self.streamState = self.streamState[string.strip(parts[1])]

        self.rpc_wait = 0
        self.rpc_expect = 0
        return

    def cbShutdownStream(self, ch, method, props, body):
        self.rpc_expect = 0
        self.shutdownRequested = True;
        if (body[0] == 'Y'):
            self.myTxid       = -1 # no stream exists
            self.myRoutekey   = []
            self.streamState  = self.statemap['COOL']


