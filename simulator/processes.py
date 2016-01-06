import logging
import simpy
import numpy
from settings import *


class Scheduler(object):
    """docstring for Scheduler"""
    def __init__(self, env, job_class, machines):
        self.env = env
        self.job_class = job_class
        self.machines = machines
        self.job_queue = simpy.Store(env)
        self.process = env.process(self.job_producer())

    def job_producer(self):
        for job in self.job_class.generator():
            if job["start_time"] > self.env.now:
                yield self.env.timeout(job["start_time"] - self.env.now)
            logging.debug("{} => Submit job {}".format(self.env.now, job["job_id"]))
            yield self.job_queue.put(job)
            yield self.env.process(self.scheduler())

    def scheduler(self):
        for _ in range(len(self.job_queue.items)):
            job = yield self.job_queue.get()
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
            logging.debug("{} => Commit job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                self.env.process(self.execute_task(job["job_id"], task))
        else:  # Rollback
            logging.debug("{} => Rollback job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                if task["machine_id"] is not None:
                    yield self.env.process(task["machine_id"].remove_task(task))
            yield self.job_queue.put(job)
            raw_input()

    def execute_task(self, job_id, task):
        # logging.debug("{} => Start job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.timeout(task["duration"])
        # logging.debug("{} => End job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.process(self.machines[task["machine_id"]].remove_task(task))


class Balancer(object):
    """docstring for Balancer"""
    def __init__(self):
        pass
