#!/usr/bin/python
#Original Author: Nick Bond
#Purpose: This script allows the user to connect to an S3 cloud storage source
#   and then create a new bucket and key. After that the user is able to
#   upload a file and append the bucket with the new key that houses their
#   file. A MySQL dump is used for this example
#
# Revised: Erik Keever
# execfile() this and you've got an open terminal to our S3 system

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

