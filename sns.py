#!/usr/bin/env python
import boto
import boto3
import boto.dynamodb2
import sys
import time
import math 

import aws
from aws import getClient, getResource, getCredentials

# GET SNS RESOURCE 
def subscribe(phoneNum):
	sns_resource = getResource('sns', 'us-east-1')
	topic = sns_resource.Topic('')
	subscription = topic.subscribe(
 	   Protocol= 'sms',
   	   Endpoint= phoneNum
	)

def notify(msg):
	sns_resource = getResource('sns', 'us-east-1')
	topic = sns_resource.Topic('')
	res = topic.publish(
    		Message=msg,
    		Subject=msg
	)

