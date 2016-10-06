RANDOM_SEED = 42
# TOTAL_MACHINES = 12580  # Implied from Google's trace
TOTAL_MACHINES = 2000  # Implied from Google's trace
TOTAL_JOBS = 604147  # From Google's trace
TOTAL_TASKS = 23411959  # From Google's trace
JOB_SAMPLE_SIZE = 1.0
#MAXIMUM_TASK_PER_JOB = 508259  # 508258+1 just to be safe
MAXIMUM_TASK_PER_JOB = 0  # No limit
MAX_RETRIES = 300  # Keep retrying for 15 minutes
CPU_SIZE = 1.0  # Normalized to 1-max
MEMORY_SIZE = 1.0  # Normalized to 1-max
OVERCOMMIT_FACTOR = 1.55
DB_CONNECTION = "mysql+pymysql://simulator:HKrbM34ChPcsHe@163.221.29.174/google?charset=utf8mb4"
JOB_CLASS = "simulator.models.GoogleJob"
#TRACEFILE = "/Users/KnightBaron/Projects/tasks.csv.gz"
TRACEFILE = "/root/tasks.csv.gz"
SCHEDULING_TIME = 3000000  # 3s
REBALANCE_TIME = 3000001  # 3s
# SIMULATION_DURATION = 2592000000000  # 30 days
SIMULATION_DURATION = 604800000000  # 7 days
SERVICE_TASK_THRESHOLD = 2000000000  # 2000s => ~30mins
MACHINE_UTILIZATION_DIFFERENCE_THRESHOLD = 0.3
COMPARABLE_TASK_THRESHOLD = 0.1
ENABLE_TASK_REBALANCING = False
#OUTPUT_FILE = "/Users/KnightBaron/Projects/simulation_output.csv"
OUTPUT_FILE = "/root/simulation_output.csv"
CONSOLIDATED_OUTPUT_FILE = "/root/consolidated_simulation_output.csv"
MONITOR_INTERVAL = 60000000  # 60s => 1mins
