# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
from threading import Thread
import random
#import boto3
#from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws
from aws import getClient, getResource, getCredentials
from mtaUpdates import mtaUpdates 
### YOUR CODE HERE ####
import boto 
#import boto.dynamodb2
import time
from time import gmtime, strftime
import threading
from datetime import datetime
from dateutil import tz

from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item

client_dynamo = None
def connect():
    global client_dynamo
    client_dynamo = getClient('dynamodb','us-east-1')
    tables = client_dynamo.list_tables()
    #if table 'Temperature' not exist, create it,
    # otherwise call Table() function to get it
    if 'mtaData' not in tables['TableNames']:
        mtaTable = Table.create('mtaData', schema=[
            HashKey('tripId'),
        
        ], throughput={
            'read':10,
            'write':10,
        },connection=client_dynamo)
        time.sleep(10)
        
    else:
        print 'getting table'
        mtaTable = Table('mtaData', connection=client_dynamo)
    
    return mtaTable


def task1(mtaTable, mtaUpdateObj):
    while(1):
        print 'task 1'
        updates = mtaUpdateObj.getTripUpdates()
        for update in updates:
            if update.vehicleData!= None:
                    Id = update.vehicleData.currentStopId
                    status = update.vehicleData.currentStopStatus
                    timeS = update.vehicleData.timestamp
            else:
                Id = None
                status = None
                timeS = None  
            resource = getResource('dynamodb', 'us-east-1')
            table = resource.Table('mtaData')
            response = table.put_item(
                    Item = {
                        'tripId': update.tripId,
                        'routeId': update.routeId,
                        'startDate': update.startDate,
                        'direction': update.direction,
                        'currentStopId': Id,
                        'currentStopStatus': status,
                        'vehicleTimeStamp': timeS,
                        'futureStopData': update.futureStops
                        }
                    )
            print 'saving'
            if update.routeId == '1':
               decision = None
               if random.choice('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabcdefghijkmlnopq') == 'a':
                   decision = 1
               else:
                   decision = 0
               write(update.tripId, update.routeId, '20161215', convertTime(int (time.time())), "weekday", "day", decision)
        time.sleep(3)

def write(tripId, routeId, startDate, time, day, date, decision):
    with open('trip.cvs', 'a') as file:
        file.write('%s,%s,%s,%s,%s,%s,%s\n' % (tripId, routeId, startDate, time, day, date, decision))
def convertTime(timeS):
    unix_time = timeS
    #hour =  datetime.fromtimestamp(int(unix_time)).strftime('%H')
    #minute =  datetime.fromtimestamp(int(unix_time)).strftime('%M')
    dateTimeObj = datetime.fromtimestamp(int(unix_time))
    fromZone = tz.tzutc()
    toZone = tz.gettz('America/New_York')
    utc = dateTimeObj.replace(tzinfo=fromZone)
    dateTimeEST = utc.astimezone(toZone)
    hour = dateTimeEST.strftime('%H')
    minute = dateTimeEST.strftime('%M')
    return (int(hour)*60+int(minute))


def task2(mtaTable):
    while(1):
        print 'task 2'
#        responses = mtaTable.scan(
#            timeStamp__lte=(time.time()-120)
#        )
#        for resp in responses:
#            res = table.delete_item(tripId=resp['tripId'])
        time.sleep(60)

if __name__ == "__main__":
    try:
        mtaUpdateObj = mtaUpdates('20778f9857669c6fdf7fbd4e4f07fd30')
        table = connect()
        t1 = threading.Thread(target=task1, args=(table,mtaUpdateObj))
        t1.setDaemon(True)
        t1.start()
        t2 = threading.Thread(target=task2, args=(table,))
        t2.setDaemon(True)
        t2.start()
        while 1:
                pass
    except KeyboardInterrupt:
        exit 

