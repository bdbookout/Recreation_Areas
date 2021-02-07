import requests
import pandas as pd  
import numpy as np
import json
import sqlalchemy
import psycopg2

api = 'f353b7e5-7d75-4c87-8657-f157e032c534'
params = {'apikey': api}

#Recreation Areas: loop to pull max json responses from API and load to dataframe
RecAreaDF = pd.DataFrame()
offval = 0
lenj = 50

while lenj == 50:

    recurl = f'https://ridb.recreation.gov/api/v1/recareas?limit=50&offset={offval}&full=false&state=WA&lastupdated=10-01-2018/'
    rarea = requests.get(recurl, params=params)
    j = rarea.json()['RECDATA']
    df = pd.json_normalize(j)
    RecAreaDF = RecAreaDF.append(df)
    offval += len(j)
    lenj = len(j)
    print(offval)
    print(lenj)

print(j)
print(RecAreaDF)

##RecAreaDF.to_csv('/Users/bari/python/onX/RecAreaDF.csv')

#
#Activities:  loop through the Recreation Area data for Washington, pull activities and write to DF
AreaIDList = list(RecAreaDF['RecAreaID'].astype('int32'))
ActivityDF = pd.DataFrame()
offval = 0

for l in AreaIDList:
    
    acturl = f'https://ridb.recreation.gov/api/v1/recareas/{l}/activities?limit=50&offset={offval}/'
    actreq = requests.get(acturl, params=params)
    j = actreq.json()['RECDATA']
    dfa = pd.json_normalize(j)
    ActivityDF = ActivityDF.append(dfa)
    offval += len(j)

#ActivityDF1 = ActivityDF[['ActivityID', 'ActivityParentID', 'RecAreaID', 'ActivityName', 'RecAreaActivityFeeDescription']]
#ActivityDF1.to_csv('/Users/bari/python/onX/ActivityDF.csv')
 
 actlist =  ActivityDF[['ActivityID', 'ActivityName']].drop_duplicates().reset_index()
 pd.DataFrame(actlist, columns=['ActivityID', 'ActivityName']).to_csv('/users/bari/python/onX/ActivitiesList.csv')

#Organization: loop through the Org information and write to dataframe
OrgDF = pd.DataFrame()
offval = 0
lenj = 50

while lenj == 50:
    
    orgurl = f'https://ridb.recreation.gov/api/v1/organizations?limit=50&offset=0/'
    orgreq = requests.get(orgurl, params=params)
    j = orgreq.json()['RECDATA']
    dfa = pd.json_normalize(j)
    OrgDF = OrgDF.append(dfa)
    offval += len(j)
    lenj = len(j)
    print(offval)
    print(lenj)

OrgDF.to_csv('/Users/bari/python/onX/OrgDF.csv')

#Facility: loop through the Org information and write to dataframe

api = 'f353b7e5-7d75-4c87-8657-f157e032c534'
parameters = {'apikey': api}

areaID = [1116, 16822, 1784]
offval = 0
lenj = 0
FacDF = pd.DataFrame()

for i in areaID:
    offval = 0
    lenj = 50
    while lenj == 50:
        facurl = f'https://ridb.recreation.gov/api/v1/recareas/{i}/facilities?limit=50&offset={offval}&lastupdated=10-01-2018/'
        facreq = requests.get(facurl, params=parameters)
        j = facreq.json()['RECDATA']
        dfc = pd.json_normalize(j)
        FacDF = FacDF.append(dfc)
        offval += len(j)
        lenj = len(j)
        print(i)
        print(offval)
        print(lenj)

FacDF1 = FacDF[FacDF['FacilityLongitude'] != 0]

FacDF1.to_csv('/Users/bari/python/onX/FacDF.csv')

#create a table in Postgresql
def create_table(cursor) -> None:
    cursor.execute("""
        DROP TABLE IF EXISTS Rec_Areas;
        CREATE TABLE Rec_Areas (

                RecAreaID  INTEGER,
                OrgRecAreaID INTEGER,
                ParentOrgID INTEGER,
                RecAreaName TEXT,
                RecAreaDescription TEXT,
                RecAreaFeeDescription TEXT,
                RecAreaDirections TEXT,
                RecAreaPhone TEXT,
                RecAreaEmail TEXT,
                RecAreaReservationURL TEXT,
                RecAreaMapURL TEXT,
                RecAreaLongitude TEXT,
                RecAreaLatitude TEXT,
                StayLimit INTEGER,
                Keywords TEXT,
                Reservable TEXT,
                Enabled TEXT,
                LastUpdatedDate DATE

                );
                """
        )
#database connnection
con = psycopg2.connect(
    host="localhost",
    database="bari",
    user="bari",
    password=None,
)
con.autocommit = True

with con.cursor() as cursor:
    create_table(cursor)

