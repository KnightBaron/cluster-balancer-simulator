import logging
import csv

INPUT = "/Users/KnightBaron/allocation_amount_7days.csv"
OUTPUT = "/Users/KnightBaron/allocation_cdf_7days.csv"
MAX_TASKS = 5442378.0
TOTAL_BUCKET = 10000

SCHEMA = ["task_count", "total_cpu", "total_memory"]

buckets = {}
for i in range(1, TOTAL_BUCKET + 1):
    buckets[(MAX_TASKS / TOTAL_BUCKET) * i] = 0

with open(INPUT) as csvfile:
    histogram_csv = csv.DictReader(csvfile, SCHEMA)
    for entry in histogram_csv:
        task_count = float(entry["task_count"])
        total_cpu = float(entry["total_cpu"])
        for key in buckets:
            if task_count <= key:
                buckets[key] += total_cpu

with open(OUTPUT, "w") as csvfile:
    output = csv.DictWriter(csvfile, SCHEMA)
    for key in buckets:
        output.writerow({
            "task_count": key,
            "total_cpu": buckets[key],
        })
