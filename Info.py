from bs4 import BeautifulSoup
import json
import cgitb
import urllib2
import cgi
import sys
import re
from datetime import datetime,timedelta
import httplib
import ssl
import socket

def GetHTTPS(url):
    xResponse = urllib2.urlopen("http://mwsecure.mobi/httpsget.php?url="+urllib2.quote(url))
    return xResponse.read()

cgitb.enable()
currentTime = datetime.now() + timedelta(hours=7)
currentHour = str(currentTime.hour).zfill(2)
currentDay = str(currentTime.day).zfill(2)
currentMonth = str(currentTime.month).zfill(2)
currentYear = str(currentTime.year).zfill(4)
prevTime = currentTime - timedelta(hours=4)
prevDay = str(prevTime.day).zfill(2)
prevMonth = str(prevTime.month).zfill(2)
prevYear = str(prevTime.year).zfill(4)
prevHour = str(prevTime.hour).zfill(2)
form = cgi.FieldStorage()
if ("campaignId" not in form):
    print "Content-Type: text/plain; charset=UTF-8"
    print
    print "Please enter an ID"
    sys.exit()
campaignId = form["campaignId"].value
html_doc = ""
fbAccessToken = GetHTTPS( "https://graph.facebook.com/oauth/access_token?client_id=810340412351004&client_secret=f76ba5d3146f920365e25d95c1bed85c&grant_type=client_credentials")
url = "http://64.27.102.169/mbuzz/search/page?from="+ prevYear + prevMonth + prevDay + prevHour +"&to=" + currentYear + currentMonth + currentDay + currentHour + "&campaignId="+ campaignId +"&offset=20000"
try:
    response = urllib2.urlopen(url)
    html_doc = response.read()
except:
    print "Content-Type: text/plain; charset=UTF-8"
    print
    print "Please enter a valid Campaign ID"
    sys.exit()
soup = BeautifulSoup(html_doc, "xml")
if soup.find("companyId").string != "19222":
    print "Content-Type: text/plain; charset=UTF-8"
    print
    print "Unauthorized campaign"
    sys.exit()
OutArray = []
for CupaSoup in soup.findAll("searchHits"):
    OutObj={}
    for SpoonFull in CupaSoup.contents:
        if SpoonFull.string != None:
            Hits = []
            if SpoonFull.name == "hitSentence":
                AllHits = re.findall(r'<span[^>]*?>(.*?)</span>', SpoonFull.string)
                for ThisHit in AllHits:
                    Hits.append(ThisHit)
                OutObj["Hits"]=Hits
            if SpoonFull.name == "themes":
                values = SpoonFull.string.split(";")
                OutObj[SpoonFull.name] = values
            else:
                OutObj[SpoonFull.name]=SpoonFull.string
        else:
            OutObj[SpoonFull.name]=""
    if OutObj["fullText"] == "":
        url = "http://mw-hb-mbuzz.meltwater.cl.datapipe.net:50080/mbuzz/search/post?length=1000000000&docId=" + urllib2.quote(OutObj["docId"], '')
        twitterDoc = urllib2.urlopen(url)
        twitterText = twitterDoc.read()
        twitterSoup = BeautifulSoup(twitterText, "xml")
        tweetText = twitterSoup.fullText.string
        OutObj["fullText"] = tweetText
    if "twitter" in OutObj["url"]:
        OutObj["profilePic"] = "https://twitter.com/"+OutObj["authorName"]+"/profile_image"
    elif "facebook.com" in OutObj["url"]:
        fbUrl = "https://graph.facebook.com/"+OutObj["sourceId"]+"?"+fbAccessToken
        print fbUrl
        fbJson = GetHTTPS(fbUrl)
        try:
            fbObject = json.JSONDecoder().decode(fbJson)
            OutObj["profilePic"] = "http://graph.facebook.com/"+fbObject["from"]["id"]+"/picture?type=square"
        except:
            hs = open("log.txt","a")
            hs.write("URL:" + fbUrl+"/n"+"JSON:"+fbJson+"/n/n")
            hs.close()
    else:
        OutObj["profilePic"] = ""
    OutArray.append(OutObj)
print "Content-Type: application/json; charset=UTF-8"
print
print json.dumps(OutArray)