#!/usr/bin/python3
##############################################################################
# read_traffic_database.py
# -----------------------
#
# The core data held by an Asus router running asuswrt-merlin for Traffic
# Analysis is in the sqlite database:
#    /jffs/.sys/TrafficAnalyzer/TrafficAnalyzer.db
#
# This script extracts data from that database, augments against
# friendly name data available from other sources, and provides the output
# in a form suitable for use by Influxdb.
#
# Or something like that, anyway.
##############################################################################
import sqlite3
import time
from os.path import isfile
import os
from sys import argv
import json
from influxdb import InfluxDBClient
from CustClientListParser import CustClientListParser
from NtCenterMacParser import NtCenterMacParser
from RStatsDataExtract import TomatoData

#################################################################################
# USER-MODIFIABLE PARAMETERS:
# (must be in same directory as script itself)
optionsFile="read_traffic_database_options.json"
#default options we EXPECT to be overridden:
opt = {
    'inHost'        : 'influxserver',
    'inPort'        : 8086,
    'inUser'        : 'admin_user',
    'inPassword'    : 'admin_password',
    'inDatabase'    : 'routerdata',
    'inMeasurement' : 'traffic',
    'ntDBFile'      : 'sampledata/nt_center.db',
    'clientJSONfile': 'sampledata/custom_clientlist',
    'trafDataDB'    : 'sampledata/TrafficAnalyzer.db',
    'rstatsDailyM'  : 'rstatsDaily',
    'rstatsMonthM'  : 'rstatsMonthly',
    'rstatsfile'    : 'sampledata/tomato_rstats_blahblah.gz'
}

#################################################################################
class TrafficAnalyzerExtractor(object):
    """Extractor for data from the Traffic Analyzer db passed as a file name"""
    #Simple enumeration of the fields available in the traffic table
    #used for queries later...
    traffic_fields = "timestamp,mac,app_name,cat_name,tx,rx"

    def __init__(self,trafficdbfile):
        """Load and actually do the parsing of the SQLLite database"""
        self.uniquemacs = []
        self.uniqueapps = []
        self.uniquecats = []
        self.mindate = -1
        self.maxdate = 1
        self.numrows = 0
        self.dbfile = trafficdbfile
        #We want this to cause program oopsies if it fails:
        self.conn = sqlite3.connect(self.dbfile)
        #Load the unique identifiers from the table
        self._loadMetadata()

    def __del__(self):
        """Need a destructor to ensure database is closed properly"""
        self.conn.close()

    def _loadMetadata(self):
        """Populate the uniquemacs, apps, cats lists with data from the DB"""
        #We don't wrap these in try/except because we want them to die if
        #they fail.
        macCur = self.conn.execute("SELECT DISTINCT(mac) FROM traffic")
        for m in macCur:
            self.uniquemacs.append(m[0])
        appCur = self.conn.execute("SELECT DISTINCT(app_name) FROM traffic")
        for a in appCur:
            self.uniqueapps.append(a[0])
        catCur = self.conn.execute("SELECT DISTINCT(cat_name) FROM traffic")
        for c in catCur:
            self.uniquecats.append(c[0])
        tsCur = self.conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM traffic")
        for t in tsCur:
            self.mindate = t[0]
            self.maxdate = t[1]
        rcCur = self.conn.execute("SELECT COUNT(*) FROM traffic")
        for r in rcCur:
            self.numrows = r[0]

    def getAllMetrics(self):
        """This is the simplest retrieval - just all data"""
        return self._fmtQRes(self.conn.execute(f"SELECT {self.traffic_fields} FROM traffic"))

    def getAllMetricsAfter(self,minTimestamp):
        """Simple filter - get all records AFTER a given timestamp"""
        return self._fmtQRes(self.conn.execute(f"SELECT {self.traffic_fields} FROM TRAFFIC WHERE timestamp > {int(minTimestamp)}"))

    def _fmtQRes(self, query_cursor) :
        """Bit of a reusable internal"""
        res = []
        for r in query_cursor:
            res.append({
                'ts'  : r[0],
                'mac' : r[1],
                'app' : r[2],
                'cat' : r[3],
                'tx'  : r[4],
                'rx'  : r[5]
            })
        return res

    def fmtMetaData(self):
        """Pretty-print (restructure) object metadata"""
        return { 'earliestData' : self.mindate,
                 'latestData'   : self.maxdate,
                 'uniqueMACs'   : self.uniquemacs,
                 'uniqueApps'   : self.uniqueapps,
                 'uniqueCats'   : self.uniquecats,
                 'totalRows'    : self.numrows}
#################################################################################
def fmtTimeStamp(ts) :
    t = time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S", t)

###############################################################################
def loadParseJSONFile(path_to_json_file) :
    """Load a data structure from JSON stored in an external file. Returns
       either the data structure or None if there's an error"""
    try:
        with open(path_to_json_file) as jf:
            try:
                readInJSON = json.loads(jf.read())
            except Exception as e:
                print(f'WARN Could not parse JSON from file {path_to_json_file}, {e}')
                return None
    except Exception as e:
        print(f'Could not read file {path_to_json_file}, {e}')
        return None

    return readInJSON

#################################################################################
def setupInfluxConnection():
    """Setup connection to Influx database and make sure database exists/is usable"""
    client = InfluxDBClient(host     = opt['inHost'],
                            port     = opt['inPort'],
                            username = opt['inUser'],
                            password = opt['inPassword'])
    dblist = [i['name'] for i in client.get_list_database()]
    if opt['inDatabase'] not in dblist:
        print(f"Creating database {opt['inDatabase']}")
        client.create_database(opt['inDatabase'])
    #Switch to this database
    client.switch_database(opt['inDatabase'])
    client.create_retention_policy("std", "365d", "1", database=opt['inDatabase'], default=True)
    client.create_user("q","q",admin=False)
    client.grant_privilege("read",opt['inDatabase'],"q")
    return client

#################################################################################
def fluxEscapeString(inString) :
    #Influx doesn't want quotes used. Instead, use backslashes:
    return inString.replace(" ", "\ ")

#################################################################################
def fmtTrafficDataPoint(metric, macNameList):
    """Helper function to turn a metric measurement into an Influx line-protocol insert"""
    #Setup measurement and tags:
    if metric['mac'] in macNameList :
        fname = macNameList[metric['mac']]
    else :
        fname = metric['mac']
    dpoint = ",".join(
        [ opt['inMeasurement'],
          f"mac={fluxEscapeString(metric['mac'])}",
          f"app={fluxEscapeString(metric['app'])}",
          f"cat={fluxEscapeString(metric['cat'])}",
          f"name={fluxEscapeString(fname)}"
        ])
    #Append field keys:
    dpoint += f" tx={metric['tx']},rx={metric['rx']}"
    #Append timestamp
    dpoint += f" {metric['ts']}"
    #Store.
    return(dpoint)

#################################################################################
def fmtRstatDataPoint(metric,measurement):
    """Helper function to turn an rstat measurement into an Influx line-protocol insert"""
    #Almost trivial but worth functionalising for later reuse.
    dpoint = f"{measurement} rx={metric['down']},tx={metric['up']} {metric['date']}"
    return dpoint

#################################################################################
def reconcileMacNameLists(masterList,auxList) :
    """Helper function to coalesce 2 name lists together as the intersection of both"""
    #Reconcile the two above taking custom as master
    for m in auxList:
        if m not in masterList:
            masterList[m] = auxList[m]
    return masterList

#################################################################################
def getLatestRecordForMeasurement(measurement, dbconn):
    """Helper function since we seem to do this a lot...."""
    tq=dbconn.query(f"select * from {measurement} ORDER BY time desc limit 1", epoch="s")
    if len(list(tq.get_points())) > 0 :
        latestInfluxData = int(list(tq.get_points())[0]['time'])
    else :
        latestInfluxData = 0
    return latestInfluxData

#################################################################################
def updateInfluxTrafficHistory(trafficDataFile, dbconn) :
    """Wrapper for the whole process of updating the TrafficHistory measurement"""
    tdata=TrafficAnalyzerExtractor(trafficDataFile)
    macNameList = reconcileMacNameLists(
                    CustClientListParser(opt['clientJSONfile']).getMappings(),
                    NtCenterMacParser(opt['ntDBFile']).getMappings())
    missingNames=0
    for m in tdata.uniquemacs:
        if m not in macNameList:
            missingNames += 1

    #Dump the metadata from the stats we've got
    print(f"Traffic Data file:  {trafficDataFile}")
    print(f"Friendly Names:     {len(tdata.uniquemacs)-missingNames} of {len(tdata.uniquemacs)} devices")
    print(f"Date Range:         {fmtTimeStamp(tdata.mindate)} - {fmtTimeStamp(tdata.maxdate)}")
    print(f"Traffic Analyzer:   {len(tdata.uniqueapps)} applications, {len(tdata.uniquecats)} categories")
    print(f"Total Records:      {tdata.numrows} traffic records")

    #Get the "latest" timestamp from the db:
    latestInfluxData = getLatestRecordForMeasurement(opt['inMeasurement'], dbconn)

    print(f"Influx latest:      {fmtTimeStamp(latestInfluxData)}")
    #Check how much Metric data we have after this:
    metricDataNewerThanInflux = tdata.getAllMetricsAfter(int(latestInfluxData))
    print(f"Records to add:     {len(metricDataNewerThanInflux)}")

    #Do we actually need to do anything?
    if len(metricDataNewerThanInflux) < 1 :
        print(f"No new Traffic data to add to Influx")
        return

    #They really don't want you writing in JSON format these days, which is a shame
    datapoints = []
    for metric in metricDataNewerThanInflux :
        datapoints.append(fmtTrafficDataPoint(metric, macNameList))

    #Here's where the insert goes
    try:
        sT = time.perf_counter()
        dbconn.write_points(datapoints, time_precision='s', batch_size=1000, protocol='line')
        eT = time.perf_counter()
        elapsed = eT - sT
        rate = float(len(datapoints)) / elapsed
        print(f"Wrote {len(datapoints)} rows to measurement {opt['inMeasurement']} in {elapsed} seconds ({rate} rows/second)")
    except InfluxDBClientError as e:
        print(f"ERROR writing datapoints to {opt['inMeasurement']}, error = {e}")

    return


#################################################################################
def updateRStatsMeasurement(tomatofile, dbconn) :

    print(f"RStats(Tomato) File {tomatofile}")
    rStats = TomatoData(tomatofile)
    dMin,dMax = rStats.getDailyRange()
    mMin,mMax = rStats.getMonthlyRange()
    print(f"Tomato Daily range: {fmtTimeStamp(dMin)} - {fmtTimeStamp(dMax)}")
    print(f"Tomato Month range: {fmtTimeStamp(mMin)} - {fmtTimeStamp(mMax)}")

    #Create an insert based on whether we've got new data to load:
    #NB: There can be multi-insert issues where a "partial record" is stored
    #    in influx that's been later updated on disk. So we actually need to
    #    pretty much always put the few rows of each type of data in.
    newRows = []
    for r in sorted(rStats.getDaily(), key=lambda k: k['date'], reverse=True)[:3]:
        newRows.append(fmtRstatDataPoint(r, opt['rstatsDailyM']))
    for r in sorted(rStats.getMonthly(), key=lambda k: k['date'], reverse=True)[:3]:
        newRows.append(fmtRstatDataPoint(r, opt['rstatsMonthM']))
    print(f"Rows to insert/update: {len(newRows)}")
    if len(newRows)>0 :
        try:
            dbconn.write_points(newRows, time_precision='s', batch_size=1000, protocol='line')
        except InfluxDBClientError as e:
            print(f"ERROR writing RStats datapoints: {e}")

    return

#################################################################################
#################################################################################
#################################################################################
if __name__ == "__main__":
    #Override options from pref file
    scriptPath = os.path.dirname(os.path.realpath(__file__))
    fqOptionsFile = scriptPath + "/" + optionsFile
    print(f"fqOptionsFile {fqOptionsFile}")
    opt=loadParseJSONFile(fqOptionsFile)

    #Handle being passed a traffic database on the command line:
    if len(argv) != 2 or not isfile(argv[1]):
        tDataFile = opt['trafDataDB']
    else :
        tDataFile = argv[1]

    #Connect to the database
    dbconn=setupInfluxConnection()
    #Run the update process based on extracted data:
    updateInfluxTrafficHistory(tDataFile,dbconn)

    #Update the overall (rStats) Statistics
    updateRStatsMeasurement(opt['rstatsfile'],dbconn)

    #End of script
    dbconn.close()
