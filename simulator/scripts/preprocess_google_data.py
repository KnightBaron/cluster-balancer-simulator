import logging
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import text
from sqlalchemy import and_, func

# DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
DB_CONNECTION = "mysql+mysqldb://simulator:HKrbM34ChPcsHe@127.0.0.1/google?charset=utf8mb4"
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
        return connection.execute(text(query).yield_per(10))

if __name__ == "__main__":
    db = create_engine(DB_CONNECTION)
    connection = db.connect()

    results = execute("SELECT * FROM `task_usage` LIMIT 100", connection)
    for r in results:
        print r
