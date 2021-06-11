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
from CustClientListParser import CustClientListParser
from NtCenterMacParser import NtCenterMacParser

#################################################################################
class TrafficAnalyzerExtractor(object):
    """Extractor for data from the Traffic Analyzer db passed as a file name"""
    def __init__(self,trafficdbfile):
        """Load and actually do the parsing of the SQLLite database"""
        self.uniquemacs = []
        self.uniqueapps = []
        self.uniquecats = []
        self.mindate = -1
        self.maxdate = 1
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

    def fmtMetaData(self):
        """Pretty-print (restructure) object metadata"""
        return { 'earliestData' : self.mindate,
                 'latestData'   : self.maxdate,
                 'uniqueMACs'   : self.uniquemacs,
                 'uniqueApps'   : self.uniqueapps,
                 'uniqueCats'   : self.uniquecats}
#################################################################################
def fmtTimeStamp(ts) :
    t = time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S", t)

#################################################################################
#################################################################################
#################################################################################
if __name__ == "__main__":
    if len(argv) != 2 or not isfile(argv[1]):
        print("Call with the name of the Traffic Analyzer db as parameter")
        exit(1)
    tdata=TrafficAnalyzerExtractor(argv[1])
    macNameList=CustClientListParser("sampledata/custom_clientlist").getMappings()
    ntlist=NtCenterMacParser("sampledata/nt_center.db").getMappings()
    #Reconcile the two above taking custom as master
    for m in ntlist:
        if m not in macNameList:
            macNameList[m] = ntlist[m]
    missingNames=0
    for m in tdata.uniquemacs:
        if m not in macNameList:
            missingNames += 1

    print(f"I know the names of {len(tdata.uniquemacs)-missingNames} out of {len(tdata.uniquemacs)} devices")
    print(f"I have data from {fmtTimeStamp(tdata.mindate)} to {fmtTimeStamp(tdata.maxdate)}")
    print(f"And I know about {len(tdata.uniqueapps)} applications across {len(tdata.uniquecats)} categories")
