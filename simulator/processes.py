import logging
import simpy
import numpy
import itertools
from settings import *


class Scheduler(object):
    """docstring for Scheduler"""
    def __init__(self, env, job_class, machines):
        self.env = env
        self.job_class = job_class
        self.job_tracker = {}
        self.task_tracker = []
        self.machines = machines
        self.job_queue = simpy.Store(env)
        self.producer_proc = env.process(self.producer())
        self.scheduler_proc = env.process(self.scheduler())
        if ENABLE_TASK_REBALANCING:
            self.rebalancer_proc = env.process(self.rebalancer())
        self.stats = {
            "jobs": 0,
            "tasks": 0,
            "finished_jobs": 0,
            "finished_tasks": 0,
            "service_tasks": 0,
        }

    def get_success_rate(self):
        return float(self.stats["finished_jobs"]) / self.stats["jobs"]

    def rebalancer(self):
        """
        Task Rebalancer Process
        """
        while True:
            yield self.env.timeout(REBALANCE_TIME)

            candidate_mid_pair = None
            candidate_difference = 0

            for mid_pair in itertools.combinations(range(len(self.machines)), 2):
                mid_pair = list(mid_pair)
                mid_pair = sorted(mid_pair, key=lambda x: self.machines[x].actual_cpu.level)
                machine_lower = self.machines[mid_pair[0]]
                machine_higher = self.machines[mid_pair[1]]
                difference = machine_higher.actual_cpu.level - machine_lower.actual_cpu.level
                if difference > candidate_difference:
                    candidate_difference = difference
                    candidate_mid_pair = mid_pair

            if candidate_difference > MACHINE_UTILIZATION_DIFFERENCE_THRESHOLD:
                lower_machine = self.machines[candidate_mid_pair[0]]
                higher_machine = self.machines[candidate_mid_pair[1]]
                lower_tasks = []
                higher_tasks = []
                for task in self.task_tracker:
                    if task["machine_id"] == candidate_mid_pair[0]:
                        lower_tasks.append(task)
                    elif task["machine_id"] == candidate_mid_pair[1]:
                        higher_tasks.append(task)

                candidate_lower_task = None
                candidate_higher_task = None
                candidate_difference = 0
                if (len(lower_tasks) > 0) and (len(higher_tasks) > 0):
                    for task_pair in itertools.product(lower_tasks, higher_tasks):
                        lower_task = task_pair[0]
                        higher_task = task_pair[1]
                        if ((lower_task["actual_cpu"] < higher_task["actual_cpu"]) and
                                (abs(lower_task["allocated_cpu"] - higher_task["allocated_cpu"]) < COMPARABLE_TASK_THRESHOLD)):
                            difference = higher_task["actual_cpu"] - lower_task["actual_cpu"]
                            if difference > candidate_difference:
                                candidate_lower_task = lower_task
                                candidate_higher_task = higher_task

                if (candidate_lower_task is not None) and (candidate_higher_task is not None):
                    logging.info ("{} => Swapping...".format(self.env.now))
                    yield self.env.process(lower_machine.remove_task(candidate_lower_task))
                    yield self.env.process(higher_machine.remove_task(candidate_higher_task))
                    yield self.env.process(lower_machine.add_task(candidate_higher_task))
                    yield self.env.process(higher_machine.add_task(candidate_higher_task))
                    for task in self.task_tracker:
                        if ((task["job_id"] == lower_task["job_id"]) and
                                (task["task_index"] == lower_task["task_index"])):
                            task["machine_id"] = candidate_mid_pair[1]
                        elif ((task["job_id"] == higher_task["job_id"]) and
                                (task["task_index"] == higher_task["task_index"])):
                            task["machine_id"] = candidate_mid_pair[0]

    def producer(self):
        """
        Job Producer Process
        """
        job_counter = 0
        for job in self.job_class.generator():
            if job["start_time"] > self.env.now:
                yield self.env.timeout(job["start_time"] - self.env.now)
            job_counter += 1
            logging.debug("{} => Submit job {}".format(self.env.now, job["job_id"]))
            logging.debug("{} => Submitted jobs: {}/{}".format(self.env.now, job_counter, TOTAL_JOBS))
            self.job_tracker[job["job_id"]] = False
            self.stats["jobs"] += 1
            for task in job["tasks"]:
                self.stats["tasks"] += 1
                if task["is_service"]:
                    self.stats["service_tasks"] += 1
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
            logging.info("{} => Commit job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                if task["is_service"]:
                    task["job_id"] = job["job_id"]
                    self.task_tracker.append(task)
                self.env.process(self.execute_task(job["job_id"], task))
            self.job_tracker[job["job_id"]] = True
            self.stats["finished_jobs"] += 1
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
        for stored_task in self.task_tracker:
            if (stored_task["job_id"] == job_id) and (stored_task["task_index"] == task["task_index"]):
                self.task_tracker.remove(stored_task)
                self.env.process(self.machines[stored_task["machine_id"]].remove_task(task))
                break
        else:
            self.env.process(self.machines[task["machine_id"]].remove_task(task))
        self.stats["finished_tasks"] += 1
