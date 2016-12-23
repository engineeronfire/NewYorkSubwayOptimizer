import json,time,csv,sys
import boto
import threading
from threading import Thread
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item
import random
import boto3
from boto3.dynamodb.conditions import Key,Attr

import aws
from aws import getClient, getResource, getCredentials

DYNAMODB_TABLE_NAME = "mtaData"
mytopic_arn = 'arn:aws:sns:us-east-1:299738300456:MTA_Trip_Planner'
mtaTable = None
#Source = '213S'
Transfer = '120S'
#TimeSquare = '239S'
line = '1'
#dir = 'S' 
# prompt
def prompt():
    print ""
    print ">Available Commands are : "
    print "1. plan trip"
    print "2. subscribe to messages"
    print "3. exit"
    input = raw_input()
    return input  
#get all local trains passing through 96th 
def local(Dire):
    global mtaTable
    global Transfer
    trains = []
  #  l = mtaTable.scan(routeId__eq = line, direction__eq = Dire)
    l = mtaTable.scan(

            )
    print 'get scanner'
    for j in l:
        if Transfer in j['futureStopData']:
            trains.append(j)
    for j in trains:
	if j['currentStopId']!=None and j['currentStopId'] >Transfer:
	    trains.remove(j)
    print 'Local Train: '
    for i in trains:
        print '  ',i['routeId'], 'tripId:',i['tripId'], ', arrival time to 96:', i['futureStopData'][Transfer]['arrivalTime'], i['currentStopId']
    return trains
#get all express trains passing through 96th 
def express(Dire):
    global mtaTable
    global Transfer
    e = mtaTable.scan(routeId__between = ['2','3'], direction__eq = Dire)
    trains = []
    print 'Express Train: '
    for i in e:
        if Transfer in i['futureStopData'] : 
            trains.append(i) 
    for i in trains:
	if i['currentStopId']!=None and i['currentStopId'] >Transfer:
	    trains.remove(i)
    for i in trains:
        print '  ', i['routeId'], 'tripId:',i['tripId'], ', arrival time to 96:', i['futureStopData'][Transfer]['arrivalTime'], i['currentStopId']
    return trains
# the train I'll take at source station
def local_train_taken(Dire, Source):
    global mtaTable
    min = sys.maxint
    trains = mtaTable.scan(routeId__eq = line, direction__eq = Dire, currentStopId__lte = Source)
    train = None   
    for t in trains:
	if Source in t['futureStopData'] and t['futureStopData'][Source]['arrivalTime'] < min and t['currentStopId']!=None :
	    min = t['futureStopData'][Source]['arrivalTime']
            train = t
    if train != None:
        print 'local train taken:',train['tripId']
	return train
    min = sys.maxint
    for t in trains:
        if  Source in t['futureStopData'] and t['futureStopData'][Source]['arrivalTime'] < min :
            min = t['futureStopData'][Source]['arrivalTime']
            train = t
    return train
#After you reach 96th from your source station find & display tripId of earliest local train reaching the 96th station
def first_96_local(Dire, Time):
    global Transfer
    global mtaTable
    min = sys.maxint
    train =None
    trains = mtaTable.scan(routeId__eq = line, direction__eq = Dire)
    for t in trains:
        if  Transfer in t['futureStopData'] and Time <= t['futureStopData'][Transfer]['arrivalTime'] < min:
            min = t['futureStopData'][Transfer]['arrivalTime']
	    train = t
    return train
#After you reach 96th from your source station find & display tripId of earliest express train reaching the 96th station
def first_96_express(Dire, Time):
    global mtaTable
    global Transfer
    min = sys.maxint
    train = None
    trains = mtaTable.scan(routeId__between = ['2','3'], direction__eq = Dire)
    for t in trains:
	if  Transfer in t['futureStopData'] and Time <= t['futureStopData'][Transfer]['arrivalTime'] < min and t['currentStopId']!=None \
and t['currentStopId']<Transfer:
            min = t['futureStopData'][Transfer]['arrivalTime']
  	    train = t
    if train != None:
	return train
    min= sys.maxint   
    for t in trains:
        if  Transfer in t['futureStopData'] and Time <= t['futureStopData'][Transfer]['arrivalTime'] < min and t['currentStopId']==None:
            min = t['futureStopData'][Transfer]['arrivalTime']
            train = t
    return train

#get the arrival time to TS for local& express train    
def time_to_DS(lt, et, Destination, stime):
    local_train = lt
    express_train = et
    if Transfer == '120S':
        time_local = local_train['futureStopData'][Destination]['arrivalTime']
        time_express = express_train['futureStopData'][Destination]['arrivalTime']
        time = {'local':time_local-stime, 'express':time_express-stime}
    else :
        time_local = local_train['futureStopData'][Destination]['arrivalTime']
        time_express = express_train['futureStopData'][Transfer]['arrivalTime']
        train = first_96_local('N', time_express)
        time_express = train['futureStopData'][Destination]['arrivalTime']
        time = {'local':time_local-stime, 'express':time_express-stime}
    return time
#send to subscriber
def sendTo(topic_arn,msg,subj):
    sns_resource = getResource('sns', 'us-east-1')
    topic = sns_resource.Topic(topic_arn)
    res = topic.publish(
        Message=msg,
        Subject=subj
    )
# plan_trip
def plan_trip(Dire, Source, Destination):
        global Transfer
	trains_to_96_local = local(Dire) 		#Get a list of local trains passing through the 96th station
        trains_to_96_express = express(Dire)	#Get a list of express trains passing through the 96th station 
        train_taken = local_train_taken(Dire, Source)	#Get the train I'll take at source station
        if train_taken == None :
            print 'No local train available now, try later'
            msg = subj = 'No local train available now, try later'
            sendTo(mytopic_arn, msg, subj)
            return 0
        start_time = train_taken['futureStopData'][Source]['arrivalTime'] 
#        while train_taken['currentStopId'] != Transfer:
#	    train_taken = mtaTable.get_item(tripId = train_taken['tripId'])
        arrival_96_time = train_taken['futureStopData'][Transfer]['arrivalTime']
	first_ltrain_96 = train_taken	#After reach 96th, find & display tripId of earliest local train reaching 96st 
	first_etrain_96 = first_96_express(Dire, arrival_96_time)   #After reach 96th, find & display tripId of earliest express train reaching 96st
        if first_etrain_96 == None :
            print 'No express train available now, stay on local train'
            msg = subj = 'No express train available now, stay on local train'
            sendTo(mytopic_arn, msg, subj)
            return 0
	print 'first local train reaching 96th:', first_ltrain_96['tripId'], '     first express train reaching 96th:', first_etrain_96['tripId'] 
#Print time taken to reach 42nd in each case.	
	arrival_time = time_to_DS( first_ltrain_96, first_etrain_96, Destination, start_time)
        time_local = arrival_time['local']
        time_express = arrival_time['express']
#Print whether user should "Switch" or "Stay", and send to subscribers.
        print 'local: ',time_local, 'express: ',time_express
        if time_express < time_local:
	    print 'Switch to Express Train'
	    msg=subj='Switch to Express Train'
	else:
	    print 'Stay on in the Local Train'
	    msg=subj='Stay on in the Local Train'
	sendTo(mytopic_arn, msg, subj) 
# subscribe
def subscribe(topic_arn, protocol, endpoint ):
	sns_resource = getResource('sns', 'us-east-1')
	topic = sns_resource.Topic(topic_arn)
	subscription = topic.subscribe(
 	   Protocol= protocol,
   	   Endpoint= endpoint
	)
def plan_trip(source, destination):
    if random.choice('aaaaaaaaaaaaaaaafgh') == 'a':
        return 'Stay in the local train!'
    else:
        return 'Transfer to the express train!'
    
#main thread
def main():
    global mtaTable
    global Transfer
    global mytopic_arn
    resource_dynamo = getResource('dynamodb','us-east-1')
    mtaTable = resource_dynamo.Table(DYNAMODB_TABLE_NAME)
   
    while 1:
	input = prompt()
        if input == '1':
            dire = raw_input('uptown or downtown(S or N)?:')
            st = raw_input('Type in your source station: ')
            desti = raw_input('Type in your destination: ')
            if dire == 'N':
                Transfer = '120N'
            else:
                Transfer = '120S'
            plan_trip(dire, st, desti)
	
        elif input == '2':
	    phone = raw_input('Type in your phone number: ')	
            subscribe(mytopic_arn, 'sms', phone)
	else:
	    break
if __name__ == "__main__":
    main()
    exit
    

   

        
