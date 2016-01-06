RANDOM_SEED = 42
# TOTAL_MACHINES = 12580  # Implied from Google's trace
TOTAL_MACHINES = 10
CPU_SIZE = 1.0 / TOTAL_MACHINES  # Normalized to 1-max
MEMORY_SIZE = 1.0 / TOTAL_MACHINES  # Normalized to 1-max
DB_CONNECTION = "mysql+pymysql://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
JOB_CLASS = "simulator.models.GoogleJob"
TRACEFILE = "/root/tasks.csv.gz"
