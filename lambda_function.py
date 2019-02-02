import json
import boto3
import time

dbname = 'testdb'
logGroup = 'testGroup'
logStream = 'testStream'
numOfLines = 1

rdsClient = boto3.client('rds')
cloudWatchLogsClient = boto3.client('logs')
ssmClient = boto3.client('ssm')

#the time
posix_now = time.time()
now = int (posix_now * 1000)
twoHourAgo = int ( (posix_now - 7200) * 1000)
pastTime = twoHourAgo


def getFilePosition(fileList, fileName):
    p = -1
    for i in range(len(fileList)):
        if (fileList[i]['LogFileName'] == fileName):
            p = i
            break
    return p

def getFileName(fileList, filePosition):
    return fileList[filePosition]['LogFileName']

def writeDataToCloudWatchLogs(data, cloudWatchLogsClient, logGroupName, logStreamName):
    logStreamResponse = cloudWatchLogsClient.describe_log_streams(
                        logGroupName=logGroupName,
                        logStreamNamePrefix=logStreamName
                        )
    uploadSequenceToken = logStreamResponse['logStreams'][0]['uploadSequenceToken']
    response = cloudWatchLogsClient.put_log_events(
                logGroupName=logGroupName,
                logStreamName=logStreamName,
                logEvents=[
                    {
                        'timestamp': now,
                        'message': data
                    },
                ],
        sequenceToken=uploadSequenceToken
    )
    return response
 
    
def getLogData(dbname, logFileName, marker, numOfLines):
    rdsLogDownloadResponse = rdsClient.download_db_log_file_portion(
                            DBInstanceIdentifier=dbname,
                            LogFileName=logFileName,
                            Marker=marker,
                            NumberOfLines=numOfLines
                            )
    return rdsLogDownloadResponse
    #print('>>>>>>>>>>the content of response: ')
    #print(rdsLogDownloadResponse)
    #logFileData = rdsLogDownloadResponse.get('LogFileData')
    #print('>>>>>>>>>>the content of log file: ')
    #print(logFileData)
    #marker = rdsLogDownloadResponse.get('Marker')
    #print('>>>>>>>>>>the marker is: ' + marker)


def getLogFiles(dbname, pastTime):
    return rdsClient.describe_db_log_files(
            DBInstanceIdentifier=dbname,
            FileLastWritten=pastTime
            )['DescribeDBLogFiles']


def getLastFile(logfiles):
    return logfiles[-1]['LogFileName']
   
   
def getStartFile(logfiles):
    if (ssmClient.get_parameter(Name='startFile')['Parameter']['Value'] == 'null'):
        startFile = getLastFile(logfiles)
        setStartFile(startFile)
    elif (getFilePosition(logfiles, ssmClient.get_parameter(Name='startFile')['Parameter']['Value']) == -1):
        startFile = getLastFile(logfiles) 
        setStartMarker('0')
    else:    
        startFile = ssmClient.get_parameter(Name='startFile')['Parameter']['Value']
    return startFile
 
 
def getStartMarker():
    if (ssmClient.get_parameter(Name='startMarker')['Parameter']['Value'] == 'null'):
        startMarker = '0'
        setStartMarker('0')
    else:
        startMarker = ssmClient.get_parameter(Name='startMarker')['Parameter']['Value']
    return startMarker


def setStartFile(startFile):
    ssmClient.put_parameter(
        Name='startFile',
        Description='start to read from this file',
        Value=startFile,
        Type='String',
        Overwrite=True
    )

        
def setStartMarker(startMarker):
    ssmClient.put_parameter(
        Name='startMarker',
        Description='start to read from this Marker',
        Value=startMarker,
        Type='String',
        Overwrite=True
    )


def lambda_handler(event, context):
    
    logfiles = getLogFiles(dbname, pastTime)
    startFileName = getStartFile(logfiles)
    startFilePosition = getFilePosition(logfiles, startFileName)
    currentFilePosition = startFilePosition
    print('>>>>>>>>> the num of files is ' + str(len(logfiles)))
    print('>>>>>>>>> start file is ' + startFileName)
    print('>>>>>>>>> start file position is ' + str(startFilePosition))
    
    while (currentFilePosition <= (len(logfiles) - 1)):
        currentFileName = getFileName(logfiles, currentFilePosition)
        setStartFile(currentFileName)
        while (True):
            marker = getStartMarker()
            print('>>>>>>>>> get marker: ' + marker)
            getLogDataResponse = getLogData(dbname, currentFileName, marker, numOfLines)
            logData = getLogDataResponse['LogFileData']
            marker = getLogDataResponse['Marker']
            if (len(logData) > 0):
                print('>>>>>>> writing the following data with length ' + str(len(logData)))
                print(logData)
                writeDataToCloudWatchLogs(logData, cloudWatchLogsClient, logGroup, logStream)
                setStartMarker(marker)
                print('>>>>>>>> set marker: ' + getLogDataResponse['Marker'])
            else:
                print('>>>>>>>>> data length = 0 ')
                if (currentFilePosition != (len(logfiles) - 1)):
                    setStartMarker('0')
                    print('>>>>>>>>>>> set marker: 0')
                break
        currentFilePosition += 1
    
