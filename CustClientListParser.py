#!/usr/bin/python3
##############################################################################
# CustClientListParser.py
# -----------------------
#
# ASUS Routers - at least those running asuswrt-merlin firmware - store the
# mappings a user makes to the client list in a file nominally stored in
#   /jffs/nvram/custom_clientlist.
# This (essentially) maps MAC addresses to "friendy names". There's a couple
# of other integer fields too but I can't find documentation on what they're for.
#
# The format of this file is a *weird* angle-bracket delimiter. This script
# (Class) provides a way to turn this into standard Python Dict objects, by
# default using MAC address as the key.
#
#     Internal format of the clientlist is similar to a repetition of:
#          <Name>MacAddress>Int1>Int2>>
#     (noting that "<" and ">" are literals not representative)
###############################################################################
class CustClientListParser(object):
    """Parse an ASUS custom_clientlist string into a Dict with MAC as key"""

    def __init__(self, rawclientdata):
        """Load the datastring and parse into records"""
        self.mactoname = {}
        self.rawdata = rawclientdata
        self._parseclientdata()

    def _parseclientdata(self):
        """Actually perform the work of parsing data into a KVP Dictionary"""
        kvp = self.rawdata.split(">>") # Split on "record delimiter"
        if len(kvp)<2 :
            raise TypeError("Passed incorrect ASUS custom_clientlist data")
        for rec in kvp:
            if len(rec)==0 :
                continue #Ignore blank records
            fields = rec.split(">")
            if len(fields) < 2 :
                raise TypeError(f"Invalid record in custom_clientlist data: {rec}")
            if fields[0][:1] != "<" :
                raise ValueError(f"Invalid NAME field in record {rec} ({fields[0]})")
            if len(fields[1]) != 17 :
                raise ValueError(f"Invalid MAC field in record {rec} ({fields[1]})")
            self.mactoname[fields[1]] = fields[0][1:] #Strip the "<" before appending

    def getMappings(self):
        """Return mapping dict from parsed data"""
        return self.mactoname

if __name__ == "__main__":
    print("This is a TEST PROGRAM")
    print("(you probably don't want to be here)")
    from os.path import isfile
    from sys import argv
    if len(argv) == 2 and isfile(argv[1]):
        with open(argv[1]) as ccf:
            namemappings = CustClientListParser(ccf.read()).getMappings()
            for mac in namemappings:
                print(f"{mac} -> {namemappings[mac]}")
    else :
        print("When called directly, supply the path to an ASUS Router custom_clientlist file on the command line")
