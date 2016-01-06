import logging
import numpy
from settings import *


class Scheduler(object):
    """docstring for Scheduler"""
    def __init__(self, env, job, machines):
        self.env = env
        self.job = job
        self.machines = machines
        self.process = env.process(self.scheduler())

    def scheduler(self):
        for job in self.job.generator():
            if job["start_time"] > self.env.now:
                yield self.env.timeout(job["start_time"] - self.env.now)
            logging.debug("{} => Submit job {}".format(job["start_time"], job["job_id"]))
            yield self.env.process(self.schedule_job(job))

    def schedule_job(self, job):
        commit = True
        for task in job["tasks"]:
            for machine_id in numpy.random.permutation(TOTAL_MACHINES):
                if self.machines[machine_id].is_fit(task):
                    task["machine_id"] = machine_id
                    yield self.env.process(self.machines[machine_id].add_task(task))
                    break
            else:
                commit = False

        if commit:
            for task in job["tasks"]:
                yield self.env.process(self.execute_task(job["job_id"], task))
        else:  # Rollback
            logging.debug("{} => Rollback job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                if task["machine_id"] is not None:
                    yield self.env.process(task["machine_id"].remove_task(task))

    def execute_task(self, job_id, task):
        logging.info("{} => Start job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.timeout(task["duration"])
        logging.info("{} => End job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.process(self.machines[task["machine_id"]].remove_task(task))


class Balancer(object):
    """docstring for Balancer"""
    def __init__(self):
        pass
