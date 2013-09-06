#!/usr/bin/env python

import sys
import time
import RabbitAdapter

# Create the adapter
sender = RabbitAdapter.CloudAdapter()

# Connect to the stream manager
sender.connectToExchange(os.environ['STREAMBOSS_RABBITMQ_HOST'], os.environ['STREAMBOSS_RABBITMQ_USER'], os.environ['STREAMBOSS_RABBITMQ_PASSWORD'])

sender.streamSubscribe('barf')

sender.streamAnnounce('barf_twice', 'text_doubler')

def doublerfunc(method, props, body):
    sender.sendStreamItem("%s %s" % (body, body) )

sender.setRxCallback(doublerfunc)

try:
    while True:
	time.sleep(.5)
except KeyboardInterrupt:
    pass

sender.streamShutdown(0)

sender.waitForPikaThread()

sender.disconnectFromExchange()

# Just to be safe.
while sender.clearToQuit == 0:
    pass

quit()

