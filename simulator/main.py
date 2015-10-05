import simpy
import numpy
import logging
import sqlalchemy.engine
from pydoc import locate
from settings import *
from simulator.resources import RandomAccessFilterStore, Machine
from simulator.processes import Scheduler

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    numpy.random.seed = RANDOM_SEED
    # db = sqlalchemy.engine.create_engine(DB_CONNECTION).connect()
    db = None
    env = simpy.Environment()
    job = locate(JOB_CLASS)(db)  # Dynamically load and instantiate job class

    machines = [
        Machine(env, CPU_SIZE, MEMORY_SIZE)
        for i in range(TOTAL_MACHINES)]

    scheduler = Scheduler(env, job, machines)
    env.run()
