import simpy
import numpy
import logging
import sqlalchemy.engine
from pydoc import locate
from settings import *
from simulator.resources import Machine
from simulator.processes import Scheduler

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(message)s")
# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

if __name__ == "__main__":
    numpy.random.seed(seed=RANDOM_SEED)
    # db = sqlalchemy.engine.create_engine(DB_CONNECTION).connect()
    db = None
    env = simpy.Environment()
    job = locate(JOB_CLASS)(db)  # Dynamically load and instantiate job class

    machines = [
        Machine(env, CPU_SIZE, MEMORY_SIZE, OVERCOMMIT_FACTOR)
        for i in range(TOTAL_MACHINES)]

    scheduler = Scheduler(env, job, machines)

    if SIMULATION_DURATION > 0:
        env.run(until=SIMULATION_DURATION)
    else:
        env.run()

    print scheduler.stats
    print "JOB SCHEDULED RATE: {}".format(scheduler.get_job_success_rate())
    print "TASK SCHEDULED RATE: {}".format(scheduler.get_task_success_rate())
    print "SERVICE TASK SCHEDULED RATE: {}".format(scheduler.get_service_task_success_rate())
    print "BATCH TASK SCHEDULED RATE: {}".format(scheduler.get_batch_task_success_rate())
    print "TASK COMPLETION RATE: {}".format(scheduler.get_task_completion_rate())
    print "SERVICE TASK COMPLETION RATE: {}".format(scheduler.get_service_task_completion_rate())
    print "BATCH TASK COMPLETION RATE: {}".format(scheduler.get_batch_task_completion_rate())
