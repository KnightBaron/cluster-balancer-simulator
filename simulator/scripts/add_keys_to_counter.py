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
            "unmapped_page_cache", "total_page_cache", "maximum_memory_usage",
            "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
            "maximum_disk_io_time", "cycles_per_instruction",
            "memory_accesses_per_instruction", "sample_portion",
            "aggregation_type", "sampled_cpu_usage"], },
}
TASK_SCHEMA = [
    "cpu_rate", "canonical_memory_usage", "assigned_memory_usage",
    "unmapped_page_cache", "total_page_cache", "maximum_memory_usage",
    "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
    "maximum_disk_io_time", "cycles_per_instruction",
    "memory_accesses_per_instruction",
]

STRING_HASH_FIELDS = [
    "user", "job_name", "logical_job_name", "platform_id", "attribute_name",
    "attribute_value"]
DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
# DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@127.0.0.1/google?charset=utf8mb4"
DATA_PATH = "/home/knightbaron/cluster-balancer-simulator/data/clusterdata-2011-2/"  # Don't forget trailing slash
INSERT_PER_QUERY = 250000
DRY_RUN = False
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO)
    # level=logging.DEBUG)


def execute(query, connection):
    logging.debug("Executing: %s" % query)
    if DRY_RUN:
        return None
    else:
        return connection.execute(text(query))


def insert_keys(keys, connection):
    query = "INSERT IGNORE INTO `counter` (`job_id`, `task_index`) VALUES {};".format(
        ", ".join((  # Generator
            "({})".format(
                "{0}, {1}".format(key[0], key[1])
            ) for key in keys)))
    execute(query, connection)

    query = "INSERT IGNORE INTO `tasks` (`job_id`, `task_index`) VALUES {};".format(
        ", ".join((  # Generator
            "({})".format(
                "{0}, {1}".format(key[0], key[1])
            ) for key in keys)))
    execute(query, connection)


if __name__ == "__main__":
    db = create_engine(DB_CONNECTION)
    connection = db.connect()

    table_name = "task_usage"
    total_parts = TABLES[table_name]["total_parts"]
    for part in range(total_parts):

        # Skip processed task_events
        # if part < 225:
        #     continue

        filepath = "%s%s/part-%05d-of-%05d.csv.gz" % (
            DATA_PATH, table_name, part, total_parts)
        logging.info("Processing: %s" % filepath)

        csv = GZipCSVReader(filepath, TABLES[table_name]["schema"])

        logging.info("Inserting keys...")
        keys = set()
        for entry in csv:
            keys.add((entry["job_id"], entry["task_index"]))
            if len(keys) >= INSERT_PER_QUERY:
                insert_keys(keys, connection)
                keys = set()
        if len(keys) > 0:
            insert_keys(keys, connection)
            keys = set()

        csv.close()
