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
from sys import argv
import json
from influxdb import InfluxDBClient
from CustClientListParser import CustClientListParser
from NtCenterMacParser import NtCenterMacParser
#################################################################################
# USER-MODIFIABLE PARAMETERS:
optionsFile=os.getcwd() + "read_traffic_database_options.json"
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
    'trafDataDB'    : 'sampledata/TrafficAnalyzer.db'
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
def fmtDataPoint(metric):
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
def reconcileMacNameLists(masterList,auxList) :
    """Helper function to coalesce 2 name lists together as the intersection of both"""
    #Reconcile the two above taking custom as master
    for m in auxList:
        if m not in masterList:
            masterList[m] = auxList[m]
    return masterList

#################################################################################
#################################################################################
#################################################################################
if __name__ == "__main__":

    #Override options from pref file
    opt=loadParseJSONFile(optionsFile)

    if len(argv) != 2 or not isfile(argv[1]):
        print(f"Reading data from default {opt['trafDataDB']} database")
        tdata=TrafficAnalyzerExtractor(opt['trafDataDB'])
    else :
        print(f"Reading data from specified {argv[1]} database")
        tdata=TrafficAnalyzerExtractor(argv[1])

    macNameList = reconcileMacNameLists(
                    CustClientListParser(opt['clientJSONfile']).getMappings(),
                    NtCenterMacParser(opt['ntDBFile']).getMappings())
    missingNames=0
    for m in tdata.uniquemacs:
        if m not in macNameList:
            missingNames += 1

    #Dump the metadata from the stats we've got
    print(f"I know the names of {len(tdata.uniquemacs)-missingNames} out of {len(tdata.uniquemacs)} devices")
    print(f"I have data from {fmtTimeStamp(tdata.mindate)} to {fmtTimeStamp(tdata.maxdate)}")
    print(f"And I know about {len(tdata.uniqueapps)} applications across {len(tdata.uniquecats)} categories")
    print(f"All that is spread across {tdata.numrows} traffic records")

    #Connect to the database
    dbconn=setupInfluxConnection()
    #Get the "latest" timestamp from the db:
    tq=dbconn.query(f"select * from {opt['inMeasurement']} ORDER BY time desc limit 1", epoch="s")
    if len(list(tq.get_points())) > 0 :
        latestInfluxData = int(list(tq.get_points())[0]['time'])
    else :
        latestInfluxData = 0

    print(f"Influx has data up to {fmtTimeStamp(latestInfluxData)}")
    #Check how much Metric data we have after this:
    metricDataNewerThanInflux = tdata.getAllMetricsAfter(int(latestInfluxData))
    print(f"There are {len(metricDataNewerThanInflux)} points available that aren't in Influx.")

    #Do we actually need to do anything?
    if len(metricDataNewerThanInflux) < 1 :
        print(f"No new data. Fin.")
        dbconn.close()
        exit(0)

    #They really don't want you writing in JSON format these days, which is a shame
    datapoints = []
    for metric in metricDataNewerThanInflux :
        datapoints.append(fmtDataPoint(metric))

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
    finally:
        dbconn.close()
