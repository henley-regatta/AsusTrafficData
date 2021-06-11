# AsusTrafficData

A project to (basically) replicate the "Traffic Statistics" page from an
ASUS router running **Asuswrt-Merlin** software with the (Trend Micro)
Traffic Analyzer enabled.

Basically an exercise in taking the data out of the router, shuffling it into
another datastore, and visualising it from there.

## Source Data
The key files on the router that we'll use are:

  * `/jffs/.sys/TrafficAnalyzer/TrafficAnalyzer.db` - core Traffic Analyzer database (SQLite)
  * `/jffs/.sys/nc/nt_center.db` - ASUS Notification Center database (SQLite)
  * `/tmp/clientlist.json` - "Live" record of devices connected to router and how (JSON)
  * `/jffs/nvram/custom_clientlist` - File-of-Record used by the router to map MAC addresses to friendly names (Weird angle-bracket delimiting). Irritatingly, not _All_ known names are defined herein; names reported to the router by (presumably) MDNS/Avahi or DHCP reservations don't appear here.
