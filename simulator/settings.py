RANDOM_SEED = 42
# TOTAL_MACHINES = 12580  # Implied from Google's trace
TOTAL_MACHINES = 10  # Implied from Google's trace
TOTAL_JOBS = 604147  # From Google's trace
TOTAL_TASKS = 23411959  # From Google's trace
CPU_SIZE = 1.0  # Normalized to 1-max
MEMORY_SIZE = 1.0  # Normalized to 1-max
OVERCOMMIT_FACTOR = 1.2
DB_CONNECTION = "mysql+pymysql://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
JOB_CLASS = "simulator.models.GoogleJob"
TRACEFILE = "/Users/KnightBaron/Projects/tasks.csv.gz"
SCHEDULING_TIME = 100000
SIMULATION_DURATION = 10000000000  # Set to 0 or less to run until finish
