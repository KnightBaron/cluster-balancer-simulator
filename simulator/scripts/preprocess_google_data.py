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
STRING_HASH_FIELDS = [
    "user", "job_name", "logical_job_name", "platform_id", "attribute_name",
    "attribute_value"]
TASK_SCHEMA = [
    "start_time", "end_time",  # "duration" will be calculated at the end
    # "cpu_request", "memory_request", "disk_space_request",
    "cpu_rate", "canonical_memory_usage", "assigned_memory_usage",
    "unmapped_page_cache", "total_page_cache", "maximum_memory_usage",
    "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
    "maximum_disk_io_time", "cycles_per_instruction",
    "memory_accesses_per_instruction",
]
DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
# DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@127.0.0.1/google?charset=utf8mb4"
DATA_PATH = "/home/knightbaron/cluster-balancer-simulator/data/clusterdata-2011-2/"  # Don't forget trailing slash
INSERT_PER_QUERY = 800000  # Actually, UPDATE_PER_TRANSACTION
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


if __name__ == "__main__":
    db = create_engine(DB_CONNECTION)
    connection = db.connect()

    ##
    # Read and process data from csv
    ##

    table_name = "task_usage"
    total_parts = TABLES[table_name]["total_parts"]
    tasks = {}
    for part in range(total_parts):

        filepath = "%s%s/part-%05d-of-%05d.csv.gz" % (
            DATA_PATH, table_name, part, total_parts)
        logging.info("Processing: %s" % filepath)

        csv = GZipCSVReader(filepath, TABLES[table_name]["schema"])

        for entry in csv:
            key = (entry["job_id"], entry["task_index"])

            if key not in tasks:  # First usage log

                # Create new task
                task = {}
                for field in TASK_SCHEMA:
                    if (field == "start_time") or (field == "end_time"):
                        task[field] = long(entry[field])
                    else:
                        if entry[field] != "":
                            task[field] = {
                                "count": 1,
                                "value": float(entry[field]),
                            }
                        else:
                            task[field] = {
                                "count": 0,
                                "value": 0.0,
                            }

                tasks[key] = task  # Add new task to tasks dict

            else:  # Subsequence usage log

                # Update task in tasks dict
                for field in TASK_SCHEMA:
                    if field == "start_time":
                        new_start_time = float(entry["start_time"])
                        if tasks[key]["start_time"] > new_start_time:
                            tasks[key]["start_time"] = new_start_time
                    elif field == "end_time":
                        new_end_time = float(entry["end_time"])
                        if tasks[key]["end_time"] < new_end_time:
                            tasks[key]["end_time"] = new_end_time
                    else:
                        if entry[field] != "":
                            tasks[key][field]["count"] += 1
                            tasks[key][field]["value"] += float(entry[field])

            # Update DB every INSERT_PER_QUERY
            if len(tasks) > (INSERT_PER_QUERY / 4):  # Divide by # of query per task

                modified_task_schema = list(TASK_SCHEMA)
                modified_task_schema.remove("start_time")
                modified_task_schema.remove("end_time")

                logging.info("Start transaction of {} update(s)...".format(INSERT_PER_QUERY))
                with connection.begin() as transaction:
                    for key in tasks:

                        # Update counter
                        query = "UPDATE `counter` SET {0} WHERE `job_id`={1} \
                            AND `task_index`={2};".format(
                            ", ".join((  # Generator
                                "`{0}`=`{0}`+{1}".format(f, tasks[key][f]["count"]) for f in modified_task_schema
                            )),
                            key[0], key[1])
                        execute(query, connection)

                        # Update start_time and end_time
                        query = "UPDATE `tasks` SET `start_time`={0} \
                            WHERE `job_id`={1} AND `task_index`={2} \
                            AND `start_time`>{0};".format(
                            tasks[key]["start_time"],
                            key[0], key[1])
                        execute(query, connection)
                        query = "UPDATE `tasks` SET `end_time`={0} \
                            WHERE `job_id`={1} AND `task_index`={2} \
                            AND `end_time`<{0};".format(
                            tasks[key]["end_time"],
                            key[0], key[1])
                        execute(query, connection)

                        # Update task
                        query = "UPDATE `tasks` SET {0} WHERE `job_id`={1} \
                            AND `task_index`={2};".format(
                            ", ".join((  # Generator
                                "`{0}`=`{0}`+{1:.14e}".format(f, tasks[key][f]["value"]) for f in modified_task_schema
                            )),
                            key[0], key[1])
                        execute(query, connection)

                logging.info("Committed.")
                tasks = {}  # Reset tasks

        csv.close()

    if len(tasks) > 0:
        # Commit the last batch
        modified_task_schema = list(TASK_SCHEMA)
        modified_task_schema.remove("start_time")
        modified_task_schema.remove("end_time")

        logging.info("Start transaction of {} update(s)...".format(len(tasks)))
        with connection.begin() as transaction:
            for key in tasks:

                # Update counter
                query = "UPDATE `counter` SET {0} WHERE `job_id`={1} \
                    AND `task_index`={2};".format(
                    ", ".join((  # Generator
                        "`{0}`=`{0}`+{1}".format(f, tasks[key][f]["count"]) for f in modified_task_schema
                    )),
                    key[0], key[1])
                execute(query, connection)

                # Update start_time and end_time
                query = "UPDATE `tasks` SET `start_time`={0} \
                    WHERE `job_id`={1} AND `task_index`={2} \
                    AND `start_time`>{0};".format(
                    tasks[key]["start_time"],
                    key[0], key[1])
                execute(query, connection)
                query = "UPDATE `tasks` SET `end_time`={0} \
                    WHERE `job_id`={1} AND `task_index`={2} \
                    AND `end_time`<{0};".format(
                    tasks[key]["end_time"],
                    key[0], key[1])
                execute(query, connection)

                # Update task
                query = "UPDATE `tasks` SET {0} WHERE `job_id`={1} \
                    AND `task_index`={2};".format(
                    ", ".join((  # Generator
                        "`{0}`=`{0}`+{1:.14e}".format(f, tasks[key][f]["value"]) for f in modified_task_schema
                    )),
                    key[0], key[1])
                execute(query, connection)

        logging.info("Committed.")
        tasks = {}  # Reset tasks
