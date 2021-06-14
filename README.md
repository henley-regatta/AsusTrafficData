# AsusTrafficData

A project to (basically) replicate the "Traffic Statistics" page from an
ASUS router running **Asuswrt-Merlin** software with the (Trend Micro)
Traffic Analyzer enabled.

Basically an exercise in taking the data out of the router, shuffling it into
another datastore, and visualising it from there. The intent is that you'll
use already available visualisation infrastructures - I'm using Grafana.

More on the background and motivations for this little project here:
[Blog post describing the why of AsusTrafficData](https://www.guided-naafi.org/systemsmanagement/2021/06/14/WhyUseAsusWhenYouCanWriteYourOwn.html)

## Source Data
The key files on the router that we'll use are:

  * `/jffs/.sys/TrafficAnalyzer/TrafficAnalyzer.db` - core Traffic Analyzer database (SQLite)
  * `/jffs/.sys/nc/nt_center.db` - ASUS Notification Center database (SQLite)
  * `/tmp/clientlist.json` - "Live" record of devices connected to router and how (JSON)
  * `/jffs/nvram/custom_clientlist` - File-of-Record used by the router to map MAC addresses to friendly names (Weird angle-bracket delimiting). Irritatingly, not _All_ known names are defined herein; names reported to the router by (presumably) MDNS/Avahi or DHCP reservations don't appear here.

## Notes on Usage

### Options
Note that the script itself can either have the `opt` dict customised in-line or
via writing a personalised `read_traffic_database_options.json` file to "point"
it at the **filesystem** location of the required files, and to give it the
necessary details to connect to Influx.

### Source Data File usage
The files listed above should be available on any `asuswrt-merlin` router with
"Traffic Statistics" turned on. Getting them _from_ the Router to somewhere
where the main script in this repo -  `read_traffic_database.py` - can use them
is left as an Exercise For The Reader  because I can't think of any good ways to
put that sort of help in here securely.

I solved the problem for myself by automating an `scp` copy from the router to
a staging area prior to script execution - since TrafficAnalyzer only updates
data hourly this is fairly low-impact.

### Influx Usage
The script assumes you have an available Influx database server already setup.
It'll create the actual database, a query-user (hard-coded credentials, very
  poor security) and setup a retention policy (1 year) on first run.

There's probably a cardinality/redundancy issue with the use of the Tag keys
`mac` and `name` because in practice one is normally redundant. However I don't
exclude the possibility that device names may change over time so only the MAC
address uniquely identifies the device.

### Visualisation
I use Grafana. It's great.
