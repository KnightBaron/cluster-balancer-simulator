import logging
import re
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(message)s")
INPUT_LOG_FILE = "/home/knightbaron/2000/14_true/output.txt"
# TESTLINE = "2016-03-29 16:46:25,555 13434004478 => M:1188 T:6252372423:709 <=> M:1721 T:6252372423:433"
PATTERN = re.compile(
    r"^.+ => M:(?P<machine_0>\d+) T:(?P<job_0>\d+):(?P<task_0>\d+) <=> M:(?P<machine_1>\d+) T:(?P<job_1>\d+):(?P<task_1>\d+)$"
)

if __name__ == "__main__":
    counter = dict()
    total = 0
    with open(INPUT_LOG_FILE, "r") as input_log_file:
        logging.info("Processing {}".format(INPUT_LOG_FILE))
        for line in input_log_file:
            line = line.strip()
            match = PATTERN.match(line)
            if match is not None:
                match = match.groupdict()
                key_0 = "{}:{}".format(match["job_0"], match["task_0"])
                key_1 = "{}:{}".format(match["job_1"], match["task_1"])
                if key_0 in counter:
                    counter[key_0] += 1
                else:
                    counter[key_0] = 1
                if key_1 in counter:
                    counter[key_1] += 1
                else:
                    counter[key_1] = 1
                total += 1
                # print line
                # print match
                # raw_input()

    print "AFFECTED CONTAINERS:", len(counter)
    print "TOTAL SWAPPINGS:", total
    print "# OF CONTAINER THAT GOT SWAPPED FOR n TIMES:"
    summary = Counter(counter.values())
    for key in sorted(summary.keys()):
        print key, summary[key]
