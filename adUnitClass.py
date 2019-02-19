import tempfile
import os
import glob
import yaml
import csv
import datetime
import MySQLdb as mysql

from googleads import dfp
from googleads import errors
from datetime import date, timedelta
from luigi.s3 import S3Client

THIS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))
with open(os.path.join(THIS_DIR,'dfp_etl.yml'),'r') as f:
    CONFIG = yaml.load(f)['dfp']

class AdUnit:
    # root ad unit ID, Name
    ROOT_AD_UNIT_ID = 00000000
    ROOT_AD_UNIT_NAME = 'xxxx'

    def __init__(self, auId, name, parentId):
        self.id = auId or 0
        self.name = name or ''
        self.parentId = parentId or 0
        self.idHierarchy = [0,0,0,0,0,0]
        self.nameHierarchy = ['','','','','','']
        self.level = 0

    def getHierarchicalId(self, level):
        return self.idHierarchy[level] or ''

    def getHierarchicalName(self, level):
        return self.nameHierarchy[level] or ''

    def setHierarchicalName(self, level, adUnitName):
        if level >= 0 and level < 6:
            self.nameHierarchy[ level ] = adUnitName

    def setHierarchicalNames(self, hList):
        if hList is not None and len(hList) == 6:
            self.nameHierarchy = hList

    def setHierarchicalIds(self, hList):
        if hList is not None and len(hList) == 6:
            self.idHierarchy = hList
            # set level
            for idx, auId in enumerate(hList):
                if auId == self.id:
                    self.level = idx


    def setHierarchicalId(self, level, adUnitId):
        if level >= 0 and level < 6:
            idHierarchy[ level ] = adUnitId
            #set level
            if adUnitId == self.id:
                self.level = level

    # takes a list of AdUnit objects
    def setHierarchy(self, auList):
        for idx, au in enumerate(auList):
            self.idHierarchy[idx] = au.id
            self.nameHierarchy[idx] = au.name
            # set level
            if au.id == self.id:
                self.level = idx


def convertToAdUnit(row):
    adUnit = AdUnit(row[0], row[1], row[2])

    level = 0;
    #add the hierarchy adUnit Names if there
    hNameList = []
    for i in range(3, 9):
        if len(row) > i:
            level = i-3
            try:
                hNameList[level] = row[i]

            except:
                print "bad AdUnit" + str(level) + " Name: " + str(row[i])


    #add the hierarchy adUnit IDs if there
    hList = []
    for i in range(9, 15):
        if len(row) > i:
            level = i-9
            try:
                levelId = row[i]
                hList[level] = levelId
                if adUnit.id == levelId:
                    adUnit.level = level

            except:
                print "bad AdUnitID" + str(level) + " ID: " + str(row[i])

    adUnit.setHierarchicalNames(hNameList)
    adUnit.setHierarchicalIds(hList)
    return adUnit;



# make all queries necessary to get all parents for adUnitId
# array returned will have length of 6 and the top level parent at index 0
def getParents(adUnit, auMap):
    pList = []
    queryId = adUnit.parentId
    try:
        while queryId > 0:
            if queryId == AdUnit.ROOT_AD_UNIT_ID:
                pList.append( AdUnit(AdUnit.ROOT_AD_UNIT_ID, AdUnit.ROOT_AD_UNIT_NAME, 0) ) #The Root AdUnit
                break

            else:
                au = auMap[queryId]
                if au is not None:
                    pList.append(au)
                    queryId = au.parentId
                else:
                    #print "no adUnit for " + str(queryId)
                    break

    except:
        print "problems getting parents for " + adUnit.name

    listSize = len(pList)

    parents = []
    if listSize > 0:
        for x in xrange(listSize, 0, -1):
            parents.append(pList[x-1])
        parents.append(adUnit)  # last index

    #if len(parents) > 0:
        #print str( len(parents) ) + " parents returned."

    return parents;



def addParents(auMap):
    pList = []
    #loop through the dictionary
    for key, adUnit in auMap.iteritems():
        if key == AdUnit.ROOT_AD_UNIT_ID:
            continue   # top adUnit. No parents.

        try:
            parents = getParents(adUnit, auMap)
            # this should also set level internally
            adUnit.setHierarchy(parents)
            pList.append(adUnit)

        except:
            print "could not get parents for " + adUnit.name

    return pList;



def getAllAdUnits(client):
    get_all_query = ('SELECT id, name, parentId FROM Ad_Unit ORDER BY id ASC')
    data_downloader = client.GetDataDownloader(version=CONFIG['api']['version'])

    adUnitList = data_downloader.DownloadPqlResultToList(get_all_query)

    adUnitsMap = {}

    for row in adUnitList[1:]:
        # remove all backslashes from name field
        name = row[1].replace("\\","")
        row[1] = name

        adUnit = convertToAdUnit(row)
        adUnitsMap[adUnit.id] = adUnit

    return adUnitsMap


def saveToDb(auList):
    # open connection
    db = mysql.connect(CONFIG['mysql']['host'], CONFIG['mysql']['user'], CONFIG['mysql']['pwd'], CONFIG['mysql']['db'])
    cursor = db.cursor()

    # truncate table
    cursor.execute('TRUNCATE TABLE adUnits')
    db.commit()

    for adUnit in auList:
        auTuples = []

        tup = (adUnit.id,
        adUnit.name,
        adUnit.parentId,
        adUnit.nameHierarchy[0],
        adUnit.nameHierarchy[1],
        adUnit.nameHierarchy[2],
        adUnit.nameHierarchy[3],
        adUnit.nameHierarchy[4],
        adUnit.nameHierarchy[5],
        adUnit.idHierarchy[0],
        adUnit.idHierarchy[1],
        adUnit.idHierarchy[2],
        adUnit.idHierarchy[3],
        adUnit.idHierarchy[4],
        adUnit.idHierarchy[5],
        adUnit.level )

        auTuples.append( (tup) )

        # add the complete list to mysql
        cursor.executemany("""INSERT INTO adUnits (
                                                    AdUnitID,
                                                    Name,
                                                    ParentID,
                                                    AdUnit0,
                                                    AdUnit1,
                                                    AdUnit2,
                                                    AdUnit3,
                                                    AdUnit4,
                                                    AdUnit5,
                                                    AdUnitID0,
                                                    AdUnitID1,
                                                    AdUnitID2,
                                                    AdUnitID3,
                                                    AdUnitID4,
                                                    AdUnitID5,
                                                    Level
                                                )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", auTuples)

        db.commit()

    # close DB connections
    cursor.close()
    db.close()
    return True


def writeAsCsv(auList, outFile):
    csvRows = []
    for adUnit in auList:
        csvRows.append( [adUnit.id,
        adUnit.name,
        adUnit.parentId,
        adUnit.nameHierarchy[0],
        adUnit.nameHierarchy[1],
        adUnit.nameHierarchy[2],
        adUnit.nameHierarchy[3],
        adUnit.nameHierarchy[4],
        adUnit.nameHierarchy[5],
        adUnit.idHierarchy[0],
        adUnit.idHierarchy[1],
        adUnit.idHierarchy[2],
        adUnit.idHierarchy[3],
        adUnit.idHierarchy[4],
        adUnit.idHierarchy[5],
        adUnit.level] )

    csvFile = open(outFile, 'w')
    fileWriter = csv.writer(csvFile)
    fileWriter.writerows(csvRows)
    csvFile.close()
    return True


def load(client, loadDate, bucket):
    print "loading AdUnits"
    adUnitsMap = getAllAdUnits(client)
    parentedList = addParents(adUnitsMap)

    #remove any existing local files
    for auFile in glob.glob(CONFIG['local']['prefix']+'*'):
        os.remove(auFile)

    #Write data to .CSV
    outFile = (   os.path.dirname(CONFIG['local']['prefix']) + "/" +
                  CONFIG['local']['name'] + "_" +
                  loadDate.strftime(CONFIG['local']['date_format']) + "." +
                  CONFIG['local']['format'] )

    written = writeAsCsv(parentedList, outFile)

    #Copy to S3
    s3File = bucket + CONFIG['s3']['folder'] + loadDate.strftime(CONFIG['s3']['date_format']) + CONFIG['s3']['file']
    s3Client = S3Client() #should get authentication data from server boto config
    s3Client.put(outFile, s3File)

    print str( len(adUnitsMap) ) + " adUnit Map size. " + str( len(parentedList) ) + " parented List size."
    # save to DB
    saved = saveToDb(parentedList)

    return len(parentedList)


def main(client, loadDate, bucket):
    auCount = load(client, loadDate, bucket)

if __name__ == '__main__':
  # Initialize client object.
  dfp_client = dfp.DfpClient.LoadFromStorage(CONFIG['api']['auth_file'])
  loadDate = date.today() - timedelta(days=1)
  bucket = CONFIG['s3']['bucket']
  main(dfp_client, loadDate, bucket)