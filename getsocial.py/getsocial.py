import sys
import json
import pymongo
import time
from pymongo import MongoClient
import urllib
import urllib2
import re

reload(sys)
sys.setdefaultencoding('utf8')
def dateRange():
    toTimeString = int(time.time())*1000
    fromTimeString = toTimeString - (60*60*4*1000)
    output = {"field": "body.publishDate.date","from": fromTimeString,"to": toTimeString,"timeZone": "GMT","type": "range"}
    return(output)

def term(termStr):
    output = {"type":"any", "anyQueries":[{"field":"body.content.text","value":termStr,"type":"word"},{"field":"body.title.text","value":termStr,"type":"word"},{"field":"body.ingress.text","value":termStr,"type":"word"}]}
    return(output)

def allTerms(termList):
    output = {"type":"all", "allQueries":termList}
    return(output)

def anyTerms(termList):
    output = {"type":"any", "anyQueries":termList}
    return(output)

def noneTerms(matchTerms,noMatchTerms):
    output = {"type":"not","matchQuery":matchTerms,"notMatchQuery":anyTerms(noMatchTerms)}
    return output

def postOutput(query):
    url = 'http://mag-fh-searchservice.osl.basefarm.net:8080/searchservice/?traceId=Info4'
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    data = json.dumps(query)
    request = urllib2.Request(url, data=data)
    request.add_header("Content-Type",'application/json; charset=utf-8')
    request.get_method = lambda: method
    connection = opener.open(request)
    if connection.code == 200:
        data = connection.read()
    else:
        data = connection.read()
    return json.loads(data.replace('\\u',"\\\\u"))
def getCount(query):
    url = 'http://mag-fh-searchservice.osl.basefarm.net:8080/searchservice/?traceId=Info4'
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    data = json.dumps(query)
    request = urllib2.Request(url, data=data)
    request.add_header("Content-Type",'application/json')
    request.get_method = lambda: method
    connection = opener.open(request)
    if connection.code == 200:
        dataStr = connection.read()
    else:
        dataStr = connection.read()
    data = json.loads(dataStr)
    return data["views"]["count"]["totalCount"]
def index(req):
    form = req.form
    req.content_type = "application/json"
    reload(sys)
    sys.setdefaultencoding('utf8')
    client = MongoClient("mongodb://localhost/")
    db = client.info4
    campaignsCollection = db.campaigns
    #campaignId = 238321
    campaignId = form["campaignId"]
    campaignInfo =  campaignsCollection.find_one({"_id":campaignId})
    queryOut = {}
    if  not campaignInfo:
        print "Not Found"
    else:
        includeQuery = {}
        if len(campaignInfo["allwords"]) >0 and len(campaignInfo["anywords"]) >0:
            allQuery = []
            for thisAllTerm in campaignInfo["allwords"]:
                allQuery.append(term(thisAllTerm))
            anyQuery = []
            for thisAnyTerm in campaignInfo["anywords"]:
                for thisATermItem in term(thisAnyTerm)["anyQueries"]:
                    anyQuery.append(thisATermItem)
            allQuery.append(anyTerms(anyQuery))
            allQuery.append(dateRange())
            allQuery.append( {"field": "metaData.source.informationType","value": "social","type": "term"})
            includeQuery = allTerms(allQuery)
        elif len(campaignInfo["allwords"]) > 0:
            allQuery = []
            for thisAllTerm in campaignInfo["allwords"]:
                allQuery.append(term(thisAllTerm))
            allQuery.append(dateRange())
            allQuery.append( {"field": "metaData.source.informationType","value": "social","type": "term"})
            includeQuery = allTerms(allQuery)
        elif len(campaignInfo["anywords"]) > 0:
            allQuery = []
            anyQuery = []
            for thisAnyTerm in campaignInfo["anywords"]:
                for thisATermItem in term(thisAnyTerm)["anyQueries"]:
                    anyQuery.append(thisATermItem)
            allQuery.append(anyTerms(anyQuery))
            allQuery.append(dateRange())
            allQuery.append( {"field": "metaData.source.informationType","value": "social","type": "term"})
            includeQuery = allTerms(allQuery)
        if len(campaignInfo["nonewords"]) > 0:
            excludeQuery = []
            for thisNoneWord in campaignInfo["nonewords"]:
                excludeQuery.append(term(thisNoneWord))
            queryOut = noneTerms(includeQuery,excludeQuery)
        else:
            queryOut = includeQuery
        allResults = []
        # print json.dumps({"query":queryOut, "viewRequests": {"count": {"type": "count"}}})
        #postOutput({"query":queryOut, "viewRequests": {"count": {"type": "count"},"expressive":{"type": "sortedResultList","size": 10,"start": 0,"sortDirectives": [{"sortField": "body.publishDate.date","sortOrder": "DESC"}]}}})
        totalCount = getCount({"query":queryOut, "viewRequests": {"count": {"type": "count"}}})
        runningCount = totalCount
        ascendingCount = 0
        while runningCount > 0:
            countToGet = 20
            if runningCount <= 20:
                countToGet = runningCount
            dataIn = postOutput({"query":queryOut, "viewRequests": {"results":{"type": "sortedResultList","size": countToGet,"start": ascendingCount,"sortDirectives": [{"sortField": "body.publishDate.date","sortOrder": "DESC"}]}}})
            runningCount -= countToGet
            ascendingCount += countToGet
            for thisResult in dataIn["views"]["results"]["results"]:
                allResults.append(thisResult)
        return json.dumps({"results":allResults}).replace("\\\\u","\\u")
