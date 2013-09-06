#!/usr/bin/env python

import RabbitAdapter
import sys
import time

# Create the adapter
sender = RabbitAdapter.SensorAdapter()

# Connect to the stream manager
sender.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

print "Input 1 to create stream, 2 to reconnect: ",
y = int(sys.stdin.readline())

def streamKeyboard(): # Feed stream items to exchange in the silliest way possible
    try:
        while True:
            theline = sys.stdin.readline()
            sender.sendStreamItem(theline)
    except KeyboardInterrupt:
        pass

if y == 1: # create a new stream
    print "Input stream keyname: ",
    # Wait until stream-creation RPC finishes
    sender.streamRequestNew(sys.stdin.readline(), 'someproc', 0);
    sender.waitForPikaThread()

    if sender.myTxid > 0:
        print "Stream %s created!" % (sender.myRoutekey)
        print "Please bash face on keyboard and press enter to send test messages:"
        streamKeyboard(); 
    else:
	print "Unable to create stream!"
else:
    # This was handed back to the client by the original connect
    # It is essentially your "Yes, I have the rights to modify this stream" key
    print "Enter original transmit uuid key: ",
    sender.streamReconnect(sys.stdin.readline())
    sender.waitForPikaThread();

    print "Reconnected to stream %s!" % (sender.myRoutekey)
    streamKeyboard()


print "Exiting! Enter 0 to kill, 1 to coldsleep stream:"
# Stream shutdown; This tells the Pika thread to exit as well
sender.streamShutdown(int(sys.stdin.readline()))
sender.waitForPikaThread()

sender.disconnectFromExchange()

# Just to be safe.
while sender.clearToQuit == 0:
    pass

quit()

