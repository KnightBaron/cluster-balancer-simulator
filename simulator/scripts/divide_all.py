import logging
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import text
from sqlalchemy import and_, func

# DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@127.0.0.1/google?charset=utf8mb4"
TASK_SCHEMA = [
    "start_time", "end_time",  # "duration" will be calculated at the end
    # "cpu_request", "memory_request", "disk_space_request",
    "cpu_rate", "canonical_memory_usage", "assigned_memory_usage",
    "unmapped_page_cache", "total_page_cache", "maximum_memory_usage",
    "disk_io_time", "local_disk_space_usage", "maximum_cpu_rate",
    "maximum_disk_io_time", "cycles_per_instruction",
    "memory_accesses_per_instruction",
]
UPDATE_PER_TRANSACTION = 1000
DRY_RUN = False
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    # level=logging.INFO)
    level=logging.DEBUG)


def execute(query, connection):
    logging.debug("Executing: %s" % query)
    if DRY_RUN:
        return None
    else:
        return connection.execute(text(query))

if __name__ == "__main__":
    db = create_engine(DB_CONNECTION)
    connection = db.connect()

    is_done = False
    while not is_done:
        is_done = True  # In case we're done...
        logging.info("Fetching {} counters...".format(UPDATE_PER_TRANSACTION))
        counters = execute("SELECT * FROM `counter` \
            WHERE `used` <> 1 LIMIT {}".format(UPDATE_PER_TRANSACTION), connection)

        with connection.begin() as transaction:
            for counter in counters:
                logging.info("There are still results(s), continue crunching...")
                if is_done:
                    is_done = False

                # Set different_machine_restriction
                query = ""
                execute(query)

                # Divide all
                query = "UPDATE `tasks` SET {0} WHERE `job_id`={1} AND `task_index`={2}".format(
                    ", ".join((  # Generator

                    )),
                    counter[1], counter[2]  # job_id, task_index
                )
                execute(query)

                # Set used flag to 1
                query = "UPDATE `counter` SET `used`=1 \
                    WHERE `id`={}".format(counter[0])  # counter's id
                execute(query)

                print counter
                raw_input()

    logging.info("DONE!! Yayy!!")
