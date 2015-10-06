import logging
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import text
from simulator.utils import GZipCSVReader, grouper


TABLES = {
    "job_events": {
        "total_parts": 500,
        "schema": [
            "time", "missing_info", "job_id", "event_type", "user",
            "scheduling_class", "job_name", "logical_job_name"], },
    "task_events": {
        "total_parts": 500,
        "schema": [
            "time", "missing_info", "job_id", "task_index", "machine_id",
            "event_type", "user", "scheduling_class", "priority", "cpu_request",
            "memory_request", "disk_space_request",
            "different_machines_restriction"], },
    "machine_events": {
        "total_parts": 1,
        "schema": [
            "time", "machine_id", "event_type", "platform_id", "cpus",
            "memory"], },
    "machine_attributes": {
        "total_parts": 1,
        "schema": [
            "time", "machine_id", "attribute_name", "attribute_value",
            "attribute_deleted"], },
    "task_constraints": {
        "total_parts": 500,
        "schema": [
            "time", "job_id", "task_index", "comparison_operator",
            "attribute_name", "attribute_value"], },
    "task_usage": {
        "total_parts": 500,
        "schema": [
            "start_time", "end_time", "job_id", "task_index", "machine_id",
            "cpu_rate", "canonical_memory_usage", "assigned_memory_usage",
            "upmapped_page_cache", "total_page_cache", "maximum_memory_usage",
            "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
            "maximum_disk_io_time", "cycles_per_instruction",
            "memory_accesses_per_instruction", "sample_portion",
            "aggregation_type", "sampled_cpu_usage"], },
}

STRING_HASH_FIELDS = [
    "user", "job_name", "logical_job_name", "platform_id", "attribute_name",
    "attribute_value"]
# DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@127.0.0.1/google?charset=utf8mb4"
DATA_PATH = "/home/knightbaron/data/clusterdata-2011-2/"  # Don't forget trailing slash
INSERT_PER_QUERY = 1000
DRY_RUN = False
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO)


def execute(query, connection):
    logging.debug("Executing: %s" % query)
    if DRY_RUN:
        return None
    else:
        return connection.execute(text(query))


def insert_ids(ids, table_name, connection):
    query = "INSERT IGNORE INTO `{0}` (`id`) VALUES {1};".format(
        table_name,
        ", ".join(("({0})".format(n) for n in ids)))
    execute(query, connection)


def prepare_value(value, column):
    if column in STRING_HASH_FIELDS:
        # For string, empty string is accepted, just wrap with single quote
        return "'{0}'".format(value)
    else:
        # For number, NULL may be used
        if value != "":  # Number with actual value
            return value
        else:  # Number represented as empty string, use NULL instead
            if column == "different_machines_restriction":
                # The only column that do not accept NULL, default to 0 instead
                return "0"
            else:
                return "NULL"


if __name__ == "__main__":
    db = create_engine(DB_CONNECTION)
    connection = db.connect()

    for table_name in TABLES:

        # Reprocess task_events
        if table_name != "task_events":
            continue

        total_parts = TABLES[table_name]["total_parts"]
        for part in range(total_parts):

            # Skip processed task_events
            if part < 225:
                continue

            filepath = "%s%s/part-%05d-of-%05d.csv.gz" % (
                DATA_PATH, table_name, part, total_parts)
            logging.info("Processing: %s" % filepath)

            # 1st pass: insert jobs and machines
            logging.info("Inserting jobs and machines...")
            csv = GZipCSVReader(filepath, TABLES[table_name]["schema"])

            jobs = set()  # Contain string representation of number
            machines = set()  # Contain string representation of number

            for entry in csv:
                if ("job_id" in entry) and (entry["job_id"] != ""):
                    jobs.add(entry["job_id"])
                    if len(jobs) >= INSERT_PER_QUERY:
                        insert_ids(jobs, "jobs", connection)
                        jobs = set()
                if ("machine_id" in entry) and (entry["machine_id"] != ""):
                    machines.add(entry["machine_id"])
                    if len(machines) >= INSERT_PER_QUERY:
                        insert_ids(machines, "machines", connection)
                        machines = set()
            if len(jobs) > 0:
                insert_ids(jobs, "jobs", connection)
                jobs = set()
            if len(machines) > 0:
                insert_ids(machines, "machines", connection)
                machines = set()

            csv.close()

            # 2nd pass: insert everything else
            logging.info("Inserting contents...")
            csv = GZipCSVReader(filepath, TABLES[table_name]["schema"])

            # for entries in grouper(csv, INSERT_PER_QUERY):
            for entries in grouper(csv, 3):
                # BE CAREFUL! Grouper add None(s) to fill the last group
                query = "INSERT INTO `{0}` ({1}) VALUES {2};".format(
                    table_name,
                    ", ".join(("`{0}`".format(c) for c in TABLES[table_name]["schema"])),
                    ", ".join((  # Generator
                        "({0})".format(
                            ", ".join((  # Generator
                                prepare_value(entry[column], column)
                                for column in TABLES[table_name]["schema"])))
                        for entry in entries
                        if entry is not None)))
                execute(query, connection)

            csv.close()
