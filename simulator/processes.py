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
        self.producer_proc = env.process(self.producer())
        self.scheduler_proc = env.process(self.scheduler())

    def producer(self):
        job_counter = 0
        for job in self.job_class.generator():
            if job["start_time"] > self.env.now:
                yield self.env.timeout(job["start_time"] - self.env.now)
            job_counter += 1
            logging.debug("{} => Submit job {}".format(self.env.now, job["job_id"]))
            logging.info("{} => Submitted jobs: {}/{}".format(self.env.now, job_counter, TOTAL_JOBS))
            self.job_queue.put(job)
            # yield self.env.process(self.scheduler())

    def scheduler(self):
        while True:
            yield self.env.timeout(SCHEDULING_TIME)

            job = yield self.job_queue.get()
            yield self.env.process(self.schedule_job(job))
        # for _ in range(len(self.job_queue.items)):
        #     job = yield self.job_queue.get()
        #     yield self.env.process(self.schedule_job(job))

    def schedule_job(self, job):
        commit = True
        for task in job["tasks"]:
            for machine_id in numpy.random.permutation(TOTAL_MACHINES):
                if self.machines[machine_id].is_fit(task):
                    task["machine_id"] = machine_id
                    self.env.process(self.machines[machine_id].add_task(task))
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
                    self.env.process(self.machines[task["machine_id"]].remove_task(task))
            yield self.job_queue.put(job)

    def execute_task(self, job_id, task):
        # logging.debug("{} => Start job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.timeout(task["duration"])
        # logging.debug("{} => End job {} task {}".format(self.env.now, job_id, task["task_index"]))
        self.env.process(self.machines[task["machine_id"]].remove_task(task))


class Balancer(object):
    """docstring for Balancer"""
    def __init__(self):
        pass
