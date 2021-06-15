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

        #original used "l" but that's platform-size-dependent.
        # "q" is a long long of 8 bytes, always
        version, = struct.unpack("I0q", self.rstats_file.read(8))

        if version == TomatoData.ID_V0:
            max_monthly = 12
        elif version != TomatoData.ID_V1:
            print(f"Unknown Rstats file version, aborting ({hex(version)})")
            exit(1)

        for i in range(max_daily):
            self.daily.append(Bandwidth._make(struct.unpack("I2Q", self.rstats_file.read(24))))

        #Again, original format was "i0l" but that's 4 bytes on some platforms:
        dailyp, = struct.unpack("i0q", self.rstats_file.read(8))

        for i in range(max_monthly):
            self.monthly.append(Bandwidth._make(struct.unpack("I2Q", self.rstats_file.read(24))))

        #Another format change "i0l" -> "i0q"
        monthlyp, = struct.unpack("i0q", self.rstats_file.read(8))

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

    def _getDateRange(self, counters):
        """Helper function, return earliest & latest defined date from counter"""
        earliest=99999999999999
        latest=0
        for c in counters:
            if c.date != 0:
                cdate=self.get_date(c.date)
                if cdate > latest:
                    latest = cdate
                if cdate < earliest:
                    earliest = cdate
        return earliest, latest

    def getDaily(self) :
        return self._format_counters(self.daily)

    def getMonthly(self) :
        return self._format_counters(self.monthly)

    def getDailyRange(self):
        return self._getDateRange(self.daily)

    def getMonthlyRange(self):
        return self._getDateRange(self.monthly)

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
        print(f"Daily data range:   {bwUsage.getDailyRange()}")
        print(f"Monthly data range: {bwUsage.getMonthlyRange()}")
    else :
        print("When called directly, supply the path to a COMPRESSED ASUS RStats (tomato) file on the command line")
