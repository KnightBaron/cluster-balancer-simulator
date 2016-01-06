RANDOM_SEED = 42
TOTAL_MACHINES = 12580  # Implied from Google's trace
CPU_SIZE = 1.0 # Normalized to 1-max
MEMORY_SIZE = 1.0  # Normalized to 1-max
OVERCOMMIT_FACTOR = 1.2
DB_CONNECTION = "mysql+pymysql://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
JOB_CLASS = "simulator.models.GoogleJob"
TRACEFILE = "/Users/KnightBaron/Projects/tasks.csv.gz"
