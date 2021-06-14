#!/usr/bin/python3
##############################################################################
# NtCenterMacParser.py
# -----------------------
#
# In addition to the custom clientlist of user-added MAC->Name mappings,
# there's an SQLite database present normally at
#    /jffs/.sys/nc/nt_center.db
# which contains a table "nt_center" holding event records where the
# actual event data is a JSON object containing (amongst other things)
# additional MAC->Name mappings.
#
# This script exists to extract all MAC->Name mappings from this table
# and return as a standard Python dict.
#
###############################################################################
import sqlite3
import json

class NtCenterMacParser(object):
    """Extract all MAC->Name mappings from an ASUS nt_center.db SQLLIte database"""

    def getMappings(self):
        """Return mapping dict from parsed data"""
        return self.mactoname

    def getIPMappings(self):
        """This is a secondary data set, the source is less reliable but it's serviceable"""
        return self.mactoip

    def __init__(self,ntcenterdbfile):
        """Load and actually do the parsing of the SQLLite database"""
        self.mactoname = {}
        self.mactoip = {}
        self.dbfile = ntcenterdbfile
        self._parsentcentertable()

    def _parsentcentertable(self):
        """Connects and queries NT Center records"""
        conn = sqlite3.connect(self.dbfile)
        try:
            cursor = conn.execute("SELECT msg FROM nt_center ORDER BY tstamp ASC")
            for r in cursor:
                self._extractmacnamefromevent(r[0])
        except sqlite3.OperationalError as e:
            raise TypeError(f"{self.dbfile} does not contain nt_center records ({e})")
        finally:
            conn.close()

    def _extractmacnamefromevent(self,jsonmsg) :
        """Parses the JSON nt_center message and extracts mac,name,ip if available"""
        try:
            msg = json.loads(jsonmsg)
        except json.decoder.JSONDecodeError as e:
            return #it's an invalid record, not the end of the world. Skip it.
        if ('cname' in msg and 'macaddr' in msg and
            len(msg['cname'])>0 and len(msg['macaddr'])==17) :
            #We're parsing by date so we can ALWAYS overwrite the value with a "newer" one:
            self.mactoname[msg['macaddr']] = msg['cname']
        if ('ip' in msg and 'macaddr' in msg and
            len(msg['ip'])>0 and len(msg['macaddr'])==17) :
            #There's a bug in the data. Octet data isn't scrubbed, so if a
            #record has 2 digits in an octet but comes after a 3-digit octet,
            #it'll appear as a 3-digit octet quite frequently outside the 255
            #range. We can attempt to correct for this by scrubbing the
            #octets before loading:
            octets=msg['ip'].split('.')
            for i in range(len(octets)) :
                if int(octets[i]) > 255 :
                    octets[i] = octets[i][0:2]
            self.mactoip[msg['macaddr']] = '.'.join(octets)

if __name__ == "__main__":
    print("This is a TEST PROGRAM")
    print("(you probably don't want to be here)")
    from os.path import isfile
    from sys import argv
    if len(argv) == 2 and isfile(argv[1]):
        ntMapObj = NtCenterMacParser(argv[1])
        namemappings = ntMapObj.getMappings()
        ipmappings = ntMapObj.getIPMappings()
        print(f"Extracted {len(namemappings)} MAC-to-Name Mappings, and {len(ipmappings)} MAC-to-IP mappings")
        for mac in namemappings:
            print(f"{mac} -> {namemappings[mac]}",end="")
            if mac in ipmappings:
                print(f" ({ipmappings[mac]})")
            else :
                print(" (no IP found)")
    else :
        print("When called directly, supply the path to an ASUS Router nt_center.db file on the command line")
