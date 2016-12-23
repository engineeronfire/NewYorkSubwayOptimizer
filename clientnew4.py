import time,csv,sys
from pytz import timezone
from dateutil import tz
from datetime import datetime
import traceback
sys.path.append('../utils')
import mtaUpdates
from guppy import hpy
from llist import dllist,dllistnode
#import boto
# This script should be run seperately before we start using the application
# Purpose of this script is to gather enough data to build a training model for Amazon machine learning
# Each time you run the script it gathers data from the feed and writes to a file
# You can specify how many iterations you want the code to run. Default is 50
# This program only collects data. Sometimes you get multiple entries for the same tripId. we can timestamp the 
# entry so that when we clean up data we use the latest entry
import json,time
import boto3
import aws
import boto
from boto import kinesis
KINESIS_STREAM_NAME = "mtaStream"

from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item
# Change DAY to the day given in the feed
DAY = datetime.today().strftime("%A")
TIMEZONE = timezone('America/New_York')

global ITERATIONS
global START
global END
global first
global last
global final_result
final_result = dict()
line1 ={0:'242',1:'238',2:'231',3:'215',4:'207',5:'dyckman',6:'191',7:'181',8:'168',9:'157',10:'145',11:'137',12:'125',13:'116',14:'110',15:'103', 16:'96',17:'86',18:'79',19:'72',20:'66',21:'59',22:'50',23:'42',24:'34',25:'28',26:'23',27:'18',28:'14',29:'christopher',30:'houston', 31:'canal',32:'franklin',33:'chamber',34:'rector',35:'bowlinggreen',36:'southferry'}
line2 ={0:'wakefield',1:'nereid',2:'233',3:'225',4:'219',5:'dunhill',6:'burke',7:'allergen',8:'pelham',9:'bronx',10:'westfarms',11:'174',12:'freeman',12:'simpson',13:'intervals',14:'prospect',15:'jackson',16: '3v149',17:'135',18:'125',19:'116',20:'centralparknorth',21:'96',22:'72',23:'42',24:'34',25:'14',26:'chamber',27:'parkplace',28:'fulton',29:'wallet',30:'clark',31:'borough',32:'hoyt',33:'nevins',34:'atlantic',35:'bergen',36:'grandarmy',37:'easternparkwaybrooklyn',38:'franklin',39:'president',40:'sterling',41:'winthrop',42:'church',43:'beverly',44:'newkirk',45:'flatbush'}
line3 ={0:'harlem',1:'145',2:'135',3:'125',4:'116',5:'centralparknorth',6:'96',7:'72',8:'42',9:'34',10:'14',11:'chamber',12:'parkplace',13:'fulton',14:'wallet',15:'clark',16:'borough',17:'hoyt',18:'nevins',19:'atlantic',20:'bergen',21:'grandarmy',22:'easternparkwaybrooklyn',23:'franklin',24:'nortrand',25:'kingston',26:'sutter',27:'saratoga',28:'rockaway',29:'junius',30:'pennsylvania',31:'vasiclen',32:'newlots'}
#Default number of iterations
ITERATIONS = 5
stationNum = {'96':'120S','72':'123S','42':'127S','34':'128S','14':'132S','chamber':'137S'}
global StartNum
global EndNum

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
#################################################################
####### Note you MAY add more datafields if you see fit #########
#################################################################
# column headers for the csv file
columns =['timestamp','tripId','route','day','timeToReachExpressStation','timeToReachDestination']
dataSaved = dict()

def saveData(tripId, routeId, stopId, timeS,StartNum,EndNum):
    if tripId not in dataSaved:
        dataSaved[tripId] = {}; 
        dataSaved[tripId]["timestamp"] = None
        dataSaved[tripId]["route"] = None
        dataSaved[tripId]["day"] = None
        dataSaved[tripId]["timeToReachExpressStation"] = None
        dataSaved[tripId]["timeToReachDestination"] = None
    #get timestamp
    dataSaved[tripId]["timestamp"] = convertTime(int(time.time()))
    #get route
    if routeId == '1':
        route = 'local'
    elif routeId == '2' or routeId == '3':
        route = 'express'
    else:
        route = None
    l = {'120S':'96','123S':'72','127S':'42','128S':'34','132S':'14','137S':'chamber'}

    dataSaved[tripId]["route"] = route
    dataSaved[tripId]["StartNum"] = l[StartNum]
    dataSaved[tripId]["EndNum"] = l[EndNum]

    #get day
    now = datetime.now(timezone('America/New_York'))
    if now.today().weekday() < 5:
        dataSaved[tripId]['day'] = 'Weekday'
    else:
        dataSaved[tripId]['day'] = 'Weekend'
    #Get Time at which it reaches express station (at 96th street)
    if stopId == StartNum:
        if timeS is not None:
            dataSaved[tripId]["timeToReachExpressStation"] = convertTime(timeS) 
        else:
            dataSaved[tripId]["timeToReachExpressStation"] = None
    elif stopId == EndNum:
        if timeS is not None:
            dataSaved[tripId]["timeToReachDestination"] = convertTime(timeS)
        else:
            dataSaved[tripId]["timeToReachDestination"] = None
def writeDataToFile():
    with open('getfeed.csv','w+') as file:
        print "open file"
        MIN = sys.maxint
        start_local=''
        end_local=''
        for tripId,item in dataSaved.iteritems():
            if item['route']=='local' and item['timeToReachExpressStation']!=None and item['timeToReachDestination']!=None:
                 if int(item['timeToReachExpressStation'])> int(item['timestamp']): 
                     if int(item['timeToReachExpressStation'])<MIN:
                         MIN = item['timeToReachExpressStation']
                         start_local = item['StartNum']+'th'
                         if item['EndNum']!='chamber':
                            end_local = item['EndNum']+'th'
                         else:
                            end_local = item['EndNum']

        title = ['timestamp','tripId','route','day','start','end']
        file.write('%s,%s,%s,%s,%s,%s,%s,%s\n' % (title[0],title[1],title[2],title[3],start_local,end_local,title[4],title[5]))
        for tripId,item in dataSaved.iteritems():
            if item['timeToReachExpressStation'] == MIN and item['route']=='local'and item['timeToReachDestination']!=None:
                 file.write('%s,%s,%s,%s,%s,%s,%s,%s\n' % (item['timestamp'],tripId.split("_")[0],item['route'],item['day'],item['timeToReachExpressStation'],item['timeToReachDestination'],item['StartNum'],item['EndNum']))

def getData(mtaUpdateObj,StartNum,EndNum):
    i = ITERATIONS
    try: 
        while(i>0):
            i -= 1
            print(i)
            updates = mtaUpdateObj.getTripUpdates()
            for update in updates:
                stopId = None
                timeS = None
                 #discard the data for route other than 1 2 3
                if update.routeId!='1'and update.routeId!='2'and update.routeId!='3':
                    continue
                if hasattr(update, 'futureStops'):
                    for futureStopId in update.futureStops:
                        if StartNum == futureStopId:
                            stopId = StartNum
                            timeS = update.futureStops[futureStopId][0]['arrivalTime']
                            saveData(update.tripId, update.routeId, stopId, timeS,StartNum,EndNum)
                        elif EndNum == futureStopId: #42th street
                            stopId = EndNum
                            timeS = update.futureStops[futureStopId][0]['arrivalTime']
                            saveData(update.tripId, update.routeId, stopId, timeS,StartNum,EndNum)
                if hasattr(update,'vehicleData'):
                    if hasattr(update.vehicleData,'currentStopId'):
                        if  update.vehicleData.currentStopId == EndNum:
                            stopId = EndNum
                            if hasattr(update.vehicleData,'timestamp'): 
                                timeS = update.vehicleData.timestamp
                                saveData(update.tripId, update.routeId, stopId, timeS,StartNum,EndNum)

            updates = None
        writeDataToFile() 
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
        print("oh")
def findCommon(firstline,secondline):
    value_a = set(firstline.values())
    value_b = set(secondline.values())
    intersection = value_a & value_b
    return intersection

#find the latest intersection of two lines
def findlargest(line,line_2, inter,start,des,direction):

    Max = -1
    if direction =="downtown":
        des_key = 10000
    else:
        des_key = 0

    for key,value in line.iteritems():
        if value == start:
            start_key = key
        if value == des:
            des_key = key

    if des_key == 10000 or des_key ==0:
        
        if direction =="downtown":
            des_key2 = 0
        else:
            des_key2 = 1000

        for key2,value2 in line_2.iteritems():
            if value2 == des:
                des_key_line2 = key2

        des_value = -1 #default destination value = -1
    #step2 find the cloest
        for stops in inter:
            if stops in line_2.values():
                for key3,value3 in line_2.iteritems():
                    if stops == value3:          
                        if direction =="uptown":
                            print "key3"
                            print key3
                            print value3
                            print ">>>>>>>>>>>>>>"
                            if int(key3) > int(des_key_line2) and int(key3) <=des_key2:
                                des_key2 = key3
                                des_value =value3
    
                                
                        else:
                            
                            if int(key3)<int(des_key_line2) and int(key3) >= des_key2:
                                des_key2 = key3
                                des_value = value3

        for key4,value4 in line.iteritems():
            if value4 == des_value:
                des_key = key4
    #find the stops in the first line
    
    for i in inter:
        if i in line.values():
            for k,v in line.iteritems():
                if v == i:
                    if direction == "downtown":
                        if int(k)>Max and int(k)>start_key and int(k)<=des_key:
                            Max = int(k)
                    else:
                        if int(k)>Max and int(k)<start_key and int(k)>=des_key:
                            Max = int(k)
    return line[Max]

#find the earlest intersection of two lines
def findsmallest(line,line_2,inter,start,des,direction):
   
    Min = 100
    if direction =="downtown":
        des_key = 10000
    else:
        des_key = 0

    for key,value in line.iteritems():
        if value == start:
            start_key = key
        if value == des:
            des_key = key
    #find the cloest station near destination in intersection set
    #step1 find key value of destination in line2
    if des_key == 10000 or des_key ==0:
        
        if direction =="downtown":
            des_key2 = 0
        else:
            des_key2 = 1000

        for key2,value2 in line_2.iteritems():
            if value2 == des:
                des_key_line2 = key2

        
    #step2 find the cloest
        for stops in inter:
            if stops in line_2.values():
                for key3,value3 in line_2.iteritems():
                    if stops == value3:          
                        if direction =="uptown":
                            print "key3"
                            print key3
                            print value3
                            print ">>>>>>>>>>>>>>"
                            if int(key3) > int(des_key_line2) and int(key3) <= des_key2:
                                des_key2 = key3
                                des_value =value3
    
                                
                        else:
                            
                            if int(key3)<int(des_key_line2) and int(key3) >= des_key2:
                                des_key2 = key3
                                des_value = value3

        print start_key
        print des_value

        for key4,value4 in line.iteritems():
            if value4 == des_value:
                des_key = key4
    #find the stops in the first line
    
    for i in inter:
        if i in line.values():
            for k,v in line.iteritems():
                if v == i:
        
                    if direction == "downtown":
                        if int(k)<Min and int(k) >start_key and int(k) <= des_key:
                            Min = int(k)
                            
                    else:
                        if int(k)<Min and int(k) <start_key and int(k) >= des_key:
                            Min = int(k)
                            
    return line[Min]
                    

def checkline(startpoint,despoint,direction):
        intersection = ['96','72','42','34','14','chamber']
        message =""
        if startpoint in intersection and despoint in intersection: #do not need to switch 
            message = "Take line2 or line3, and stay on line2 and line3"
            final_result ['line'] = 'line 2 or line 3'
            final_result ['switchTo'] = 'line 2 or line 3'

        elif startpoint in line2.values() and despoint in line2.values(): #do not need to switch
            message = "Take line2, and stay on line2"
            final_result ['switchTo'] = 'line 2'
            final_result ['line'] = 'line 2'

        elif startpoint in line3.values() and despoint in line3.values(): #do not need to switch
            message = "Take line3, and stay on line3"
            final_result ['switchTo'] = 'line 3'
            final_result ['line'] = 'line 3'

        elif startpoint in line2.values() and despoint in line3.values():
            final_result ['switchTo'] = 'line 3'
            final_result ['line'] = 'line 2'
            if direction =="downtown":
                message = "Take line2, then you can switch to line3 at one of the following station "+ str(list(findCommon(line2,line3)))
                final_result['switchStop'] = str(list(findCommon(line2,line3)))
            elif direction =="uptown":
                list(findCommon(line2,line3)).reverse()
                message = "Take line2, then you can switch to line3 at one of the following station " + str(list(findCommon(line2,line3)))
                final_result['switchStop'] = str(list(findCommon(line2,line3)))
            else:
                message = "Which direction are you heading?" 
                final_result['error'] = message  

        elif startpoint in line3.values() and despoint in line2.values():
            final_result ['switchTo'] = 'line 2'
            final_result ['line'] = 'line 3'
            if direction == "downtown":
                message = "Take line3, then you can switch to line2 at one of the following station "+str(list(findCommon(line2,line3)))
                final_result['switchStop'] = str(list(findCommon(line2,line3)))

            elif direction == "uptown":
                list(findCommon(line2,line3)).reverse()
                message = "Take line3, then you can switch to line2 at one of the following station "+str(list(findCommon(line2,line3)))
                final_result['switchStop'] = str(list(findCommon(line2,line3)))
            else:
                message = "Which direction are you heading?"
                final_result['error'] = message 

        elif startpoint in line2.values() and despoint in line1.values():
            final_result ['switchTo'] = 'line 1'
            final_result ['line'] = 'line 2'
            if direction == "downtown":
                message = "Take line2, then you can switch to line1 at "+findlargest(line2,line1,findCommon(line2,line1),startpoint,despoint,direction)##find the earliest common point//tested
                final_result['switchStop']= findlargest(line2,line1,findCommon(line2,line1),startpoint,despoint,direction)
            elif direction == "uptown":
                print findCommon(line2,line1)
                message = "Take line2, then you can switch to line1 at "+findsmallest(line2,line1,findCommon(line2,line1),startpoint,despoint,direction)##find the earliest common point//tested
                final_result['switchStop'] = findsmallest(line2,line1,findCommon(line2,line1),startpoint,despoint,direction)
            else:
                message = "Which direction are you heading?"
                final_result['error'] = message

        elif startpoint in line3.values() and despoint in line1.values():
            final_result ['switchTo'] = 'line 1'
            final_result ['line'] = 'line 3'
            if direction == "downtown":
                message = "Take line3, then you can switch to line1 at "+ findlargest(line3,line1,findCommon(line1,line3),startpoint,despoint,direction)#tested
                final_result['switchStop'] = findlargest(line3,line1,findCommon(line1,line3),startpoint,despoint,direction)
            elif dirction == "uptown":
                message = "Take line3, then you can switch to line1 at "+ findsmallest(line3,line1,findCommon(line1,line3),startpoint,despoint,direction)#tested
                final_result['switchStop'] = findsmallest(line3,line1,findCommon(line1,line3),startpoint,despoint,direction)
            else:
                message = "which direction are you heading?"
                final_result['error'] = message

        elif startpoint in line1.values() and despoint in line2.values() and despoint not in intersection:
            final_result ['switchTo'] = 'line 2'
            final_result ['line'] = 'line 1'
            if direction == "downtown":
                message = "Take line1, then you can switch to line2 at " + findsmallest(line1,line2,findCommon(line2,line1),startpoint,despoint,direction)##find the earliest common point//tested
                final_result['switchStop'] = findsmallest(line1,line2,findCommon(line2,line1),startpoint,despoint,direction)
            elif direction == "uptown":
                message = "Take line1, then you can switch to line2 at " + findlargest(line1,line2,findCommon(line2,line1),startpoint,despoint,direction)##find the earliest common point//
                final_result['switchStop'] = findlargest(line1,line2,findCommon(line2,line1),startpoint,despoint,direction)
            else:
                message = "which direction are you heading?"
                final_result['error'] = message

        elif startpoint in line1.values() and despoint in line3.values() and despoint not in intersection:
            final_result ['switchTo'] = 'line 3'
            final_result ['line'] = 'line 1'
            if direction == "downtown":
                message = "Take line1, then you can switch to line3 at "+findsmallest(line1,line3,findCommon(line1,line3),startpoint,despoint,direction)#tested
                final_result['switchStop'] = findsmallest(line1,line3,findCommon(line1,line3),startpoint,despoint,direction)
            elif dirction == "uptown":
                message = "Take line1, then you can switch to line3 at "+findlargest(line1,line3,findCommon(line1,line3),startpoint,despoint,direction)
                final_result['switchStop'] = findlargest(line1,line3,findCommon(line1,line3),startpoint,despoint,direction)
            else:
                message = "which direction are you heading?"
                final_result['error'] = message

        elif startpoint in line1.values() and despoint in intersection: 
            final_result ['switchTo'] = 'line 2 or line 3'
            final_result ['line'] = 'line 1'
            if direction == "downtown":
                message = "Please check your text message. Take line1, then you can switch to line2 or line3 at "+ findsmallest(line1,line1,set(intersection),startpoint,despoint,direction)
                final_result['switchStop'] = findsmallest(line1,line1,set(intersection),startpoint,despoint,direction)
                ToAWS(findsmallest(line1,line1,set(intersection),startpoint,despoint,direction),despoint) ##tested
            elif direction == "uptown":
                message = "Please check your text message. Take line1, then you can switch to line2 or line3 at "+ findlargest(line1,line1,set(intersection),startpoint,despoint,direction)
                final_result['switchStop'] = findlargest(line1,line1,set(intersection),startpoint,despoint,direction)
                ToAWS(findlargest(line1,line1,set(intersection),startpoint,despoint,direction),despoint)
            else:
                message = "which direction are you heading?"
                final_result['error'] = message

        elif startpoint in line1.values() and despoint in line1.values() and despoint not in intersection:
            final_result ['switchTo'] = 'line 1'
            final_result ['line'] = 'line 1'
            message = "Take line1, and stay on line1"

        else:
            message = "Reminder: Please enter valid starting point and destination stop!"
            final_result['error'] = message
        print message

#Push to Kinesis
def ToAWS(stop1,stop2):
    
        first = stop1
        print first
        last = stop2
        print last
        StartNum = stationNum[first]
        EndNum = stationNum[last]
        with open('./key.txt', 'rb') as keyfile:
            APIKEY = keyfile.read().rstrip('\n')
            keyfile.close()
            mtaUpdateObj = mtaUpdates.mtaUpdates(APIKEY)
	### INSERT YOUR CODE HERE ###
            getData(mtaUpdateObj,StartNum,EndNum)
        kinesis = aws.getClient('kinesis','us-east-1')
        data = [] # list of dictionaries will be sent to kinesis
        with open('getfeed.csv','rb') as f:
        	dataReader = csv.DictReader(f)
                for row in dataReader:
                     
                     tripId_new = row['tripId']
                     if tripId_new[0] == '0':
                        print "in the loop"
                        print " remove zero"
                        row['tripId'] = tripId_new[1:] if tripId_new.startswith('0') else tripId_new
                        print row['tripId']

                     kinesis.put_record(StreamName=KINESIS_STREAM_NAME, Data=json.dumps(row), PartitionKey='0')
                     break
        f.close()
        time.sleep(2)
def connect():
    ACCOUNT_ID = 'xxxxxxxx'
    IDENTITY_POOL_ID = 'us-east-xxxxxxxxxxxxxxxxxx'
    ROLE_ARN = 'arn:aws:ixxxxxxxxxxxxxxxxxxxxxxxx'

   

    # Use cognito to get an identity.
    cognito = boto.connect_cognito_identity()
    cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
    oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

    # Further setup your STS using the code below
    sts = boto.connect_sts()
    assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])

    client_dynamo = boto.dynamodb2.connect_to_region(
        'us-east-1',
        aws_access_key_id=assumedRoleObject.credentials.access_key,
        aws_secret_access_key=assumedRoleObject.credentials.secret_key,
        security_token=assumedRoleObject.credentials.session_token)

    result_table = Table('mtanew', connection = client_dynamo)
    info = result_table.scan()
    while info == None:
        print "."    
        time.sleep(1)
            
    for item in info:
        print item['message']
        final_result['switchOrNot'] =  item['message']
        item.delete()



def main(argv):
    START= argv[0]
    END= argv[1]
    Direction = argv[2]
    checkline(START,END,Direction)
    
    print "please wait ..."
    connect()
    print final_result
    return final_result
    
if __name__ == "__main__":
    if len(sys.argv)<4:
        print "Missing arguments. Right format python clientnew.py stop1 stop2 direction(uptown or downtown)"
        sys.exit(-1)
    if len(sys.argv)>4:
        print "Extra arguments. Right format python clientnew.py stop1 stop2 direction(uptown or downtown)"
        sys.exit(-1)
    #try:
    main(sys.argv[1:])
   # except Exception as e:
    #    raise e
