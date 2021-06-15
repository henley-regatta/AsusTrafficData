#!/usr/bin/python3
##############################################################################
# RStatsDataExtract.py
# ---------------------
# Very *very* heavily based on "RStats History"  by Evan Mjelde at:
#      https://github.com/emjelde/rstats
#
# RStats (Tomato) data provides a total daily summary of traffic. It may or
# may not help reconcile the TrafficAnalyzer data which seems to suggest huge
# amounts of transfers.
###############################################################################
import gzip
import struct
from collections import namedtuple
from datetime import datetime, timezone

###############################################################################
###############################################################################
class TomatoData(object):
    """Parser for gzip compressed Tomato RStats data"""
    ID_V0 = 0x30305352
    ID_V1 = 0x31305352

    def __init__(self, filename):
        self.daily = []
        self.monthly = []
        self.rstats_file = self._decompress(filename)
        self._load_history()

    def _decompress(self,fname):
        try:
            return gzip.open(fname,'rb')
        except IOError as e:
            print(f"ERROR File {fname} could not be decompressed: {e}")
            exit(1)

    def _load_history(self):
        max_daily=62
        max_monthly=25

        Bandwidth = namedtuple('Bandwidth', 'date down up')
        daily = []
        monthly = []

        version, = struct.unpack("I0l", self.rstats_file.read(8))
        if version == TomatoData.ID_V0:
            max_monthly = 12

        for i in range(max_daily):
            self.daily.append(Bandwidth._make(struct.unpack("I2Q", self.rstats_file.read(24))))

        dailyp, = struct.unpack("i0l", self.rstats_file.read(8))

        for i in range(max_monthly):
            self.monthly.append(Bandwidth._make(struct.unpack("I2Q", self.rstats_file.read(24))))

        monthlyp, = struct.unpack("i0l", self.rstats_file.read(8))

    @staticmethod
    def get_date(xtime):
        year  = ((xtime >> 16) & 0xFF) + 1900
        month = ((xtime >>  8) & 0xFF) + 1
        day   = xtime & 0xFF
        return int(datetime(year,month, 1 if day == 0 else day,tzinfo=timezone.utc).timestamp())

    def _format_counters(self, counters):
        """Turn the named tuple values into a Dict with actual datetime"""
        outCount=[]
        for counter in counters:
            if counter.date != 0:
                cMetric = {
                    'date' : self.get_date(counter.date),
                    'up'   : counter.up,
                    'down' : counter.down
                }
                outCount.append(cMetric)
        return outCount

    def getDaily(self) :
        return self._format_counters(self.daily)

    def getMonthly(self) :
        return self._format_counters(self.monthly)

###############################################################################
###############################################################################
###############################################################################
if __name__ == "__main__":
    print("This is a TEST PROGRAM")
    print("(you probably don't want to be here)")
    from sys import argv
    from os.path import isfile
    if len(argv) == 2 and isfile(argv[1]):
        bwUsage = TomatoData(argv[1])
        print(bwUsage.getDaily())
        print(bwUsage.getMonthly())
    else :
        print("When called directly, supply the path to a COMPRESSED ASUS RStats (tomato) file on the command line")
