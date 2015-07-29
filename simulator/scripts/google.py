import sqlsoup
from simulator.utils import GZipCSVReader


db = sqlsoup.SQLSoup("sqlite+pysqlite:///../data/google.sqlite3")
csv = GZipCSVReader("/home/knightbaron/cluster-balancer-simulator/data/clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz",
    ["time", "missing_info", "job_id", "event_type", "user", "scheduling_class", "job_name", "logical_job_name"])

for entry in csv:
    print entry
    for k, y in entry:
        print k, y

print "SUCCESS!"
