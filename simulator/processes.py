import logging
import simpy
import numpy
import itertools
import csv
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
        self.monitor_proc = env.process(self.monitor())
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

    def monitor(self):
        """
        Monitor Process

        Schema: time, machine_id, allocated_cpu, actual_cpu
        Consolidated Schema: time, avg_allocated_cpu, avg_actual_cpu
        """
        with open(OUTPUT_FILE, "wb") as output, open(CONSOLIDATED_OUTPUT_FILE, "wb") as consolidated_output:
            writer = csv.writer(output)
            consolidated_writer = csv.writer(consolidated_output)
            while True:
                yield self.env.timeout(MONITOR_INTERVAL)
                avg_allocated_cpu = 0.0
                avg_actual_cpu = 0.0
                for mid in range(len(self.machines)):
                    allocated_cpu = self.machines[mid].allocated_cpu.level
                    actual_cpu = self.machines[mid].actual_cpu.level
                    avg_allocated_cpu += allocated_cpu
                    avg_actual_cpu += actual_cpu
                    writer.writerow([
                        self.env.now,
                        mid,
                        allocated_cpu,
                        actual_cpu,
                    ])
                    output.flush()
                avg_allocated_cpu /= len(self.machines)
                avg_actual_cpu /= len(self.machines)
                consolidated_writer.writerow([
                    self.env.now,
                    avg_allocated_cpu,
                    avg_actual_cpu,
                ])
                consolidated_output.flush()

    def rebalancer(self):
        """
        Task Rebalancer Process
        """

        def swappable(lower_task, higher_task, lower_machine, higher_machine):
            expected_lower_machine_allocated_cpu = \
                lower_machine.allocated_cpu.level - lower_task["allocated_cpu"] + higher_task["allocated_cpu"]
            expected_lower_machine_actual_cpu = \
                lower_machine.actual_cpu.level - lower_task["actual_cpu"] + higher_task["actual_cpu"]
            expected_higher_machine_allocated_cpu = \
                higher_machine.allocated_cpu.level - higher_task["allocated_cpu"] + lower_task["allocated_cpu"]
            expected_higher_machine_actual_cpu = \
                higher_machine.actual_cpu.level - higher_task["actual_cpu"] + lower_task["actual_cpu"]
            return (
                (expected_lower_machine_allocated_cpu >= 0) and
                (expected_lower_machine_actual_cpu >= 0) and
                (expected_higher_machine_allocated_cpu >= 0) and
                (expected_higher_machine_actual_cpu >= 0) and
                (expected_lower_machine_allocated_cpu <= lower_machine.allocated_cpu.capacity) and
                (expected_lower_machine_actual_cpu <= lower_machine.actual_cpu.capacity) and
                (expected_higher_machine_allocated_cpu <= higher_machine.allocated_cpu.capacity) and
                (expected_higher_machine_actual_cpu <= higher_machine.actual_cpu.capacity)
            )

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

                        if not swappable(
                            lower_task,
                            higher_task,
                            self.machines[lower_task["machine_id"]],
                            self.machines[higher_task["machine_id"]],
                        ):
                            continue

                        if ((lower_task["actual_cpu"] < higher_task["actual_cpu"]) and
                                (abs(lower_task["allocated_cpu"] - higher_task["allocated_cpu"]) < COMPARABLE_TASK_THRESHOLD)):
                            difference = higher_task["actual_cpu"] - lower_task["actual_cpu"]
                            if difference > candidate_difference:
                                candidate_lower_task = lower_task
                                candidate_higher_task = higher_task

                if (candidate_lower_task is not None) and (candidate_higher_task is not None):
                    logging.info("{} => Swapping...".format(self.env.now))
                    logging.info("{} => M:{} T:{}:{} <=> M:{} T:{}:{}".format(
                        self.env.now,
                        candidate_mid_pair[0],
                        lower_task["job_id"],
                        lower_task["task_index"],
                        candidate_mid_pair[1],
                        higher_task["job_id"],
                        higher_task["task_index"]
                    ))

                    logging.info("{} => Lower machine before swapping:{}".format(
                        self.env.now,
                        lower_machine
                    ))
                    logging.info("{} => Higher machine before swapping:{}".format(
                        self.env.now,
                        higher_machine
                    ))

                    # TODO: Fix negative value

                    # yield self.env.process(lower_machine.remove_task(candidate_lower_task))
                    # yield self.env.process(higher_machine.remove_task(candidate_higher_task))
                    # yield self.env.process(lower_machine.add_task(candidate_higher_task))
                    # yield self.env.process(higher_machine.add_task(candidate_higher_task))
                    #print candidate_lower_task["actual_cpu"],
                    #print candidate_lower_task["allocated_cpu"]
                    #print candidate_higher_task["actual_cpu"],
                    #print candidate_higher_task["allocated_cpu"]
                    lower_machine.remove_task(candidate_lower_task)
                    higher_machine.remove_task(candidate_higher_task)
                    lower_machine.add_task(candidate_higher_task)
                    higher_machine.add_task(candidate_lower_task)

                    logging.info("{} => Lower machine after swapping:{}".format(
                        self.env.now,
                        lower_machine
                    ))
                    logging.info("{} => Higher machine after swapping:{}".format(
                        self.env.now,
                        higher_machine
                    ))

                    for task in self.task_tracker:
                        if ((task["job_id"] == lower_task["job_id"]) and
                                (task["task_index"] == lower_task["task_index"])):
                            task["machine_id"] = candidate_mid_pair[1]
                        elif ((task["job_id"] == higher_task["job_id"]) and
                                (task["task_index"] == higher_task["task_index"])):
                            task["machine_id"] = candidate_mid_pair[0]

                    # raw_input()

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
            logging.info("{} => Submitted jobs: {}/{}".format(self.env.now, job_counter, TOTAL_JOBS))
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
                    # yield self.env.process(self.machines[machine_id].add_task(task))
                    logging.debug("{} => Try T:{}:{} on M:{}".format(
                        self.env.now, job["job_id"], task["task_index"], task["machine_id"]
                    ))
                    task["machine_id"] = machine_id
                    self.machines[machine_id].add_task(task)
                    break
            else:
                commit = False

        if commit:
            logging.debug("{} => Commit job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                logging.debug("{} => Commit T:{}:{} on M:{}".format(
                    self.env.now, job["job_id"], task["task_index"], task["machine_id"]
                ))
                if task["is_service"]:
                    task["job_id"] = job["job_id"]
                    self.task_tracker.append(task)
                self.env.process(self.execute_task(job["job_id"], task))
            self.job_tracker[job["job_id"]] = True
            self.stats["finished_jobs"] += 1
        else:  # Rollback
            logging.debug("{} => Rollback job {}".format(self.env.now, job["job_id"]))
            for task in job["tasks"]:
                logging.debug("{} => Remove T:{}:{} on M:{}".format(
                    self.env.now, job["job_id"], task["task_index"], task["machine_id"]
                ))
                if task["machine_id"] is not None:
                    # self.env.process(self.machines[task["machine_id"]].remove_task(task))
                    self.machines[task["machine_id"]].remove_task(task)
                    task["machine_id"] = None
            yield self.job_queue.put(job)

    def execute_task(self, job_id, task):
        # logging.debug("{} => Start job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.timeout(task["duration"])
        # logging.debug("{} => End job {} task {}".format(self.env.now, job_id, task["task_index"]))
        for stored_task in self.task_tracker:
            if (stored_task["job_id"] == job_id) and (stored_task["task_index"] == task["task_index"]):
                self.task_tracker.remove(stored_task)
                # self.env.process(self.machines[stored_task["machine_id"]].remove_task(task))
                self.machines[stored_task["machine_id"]].remove_task(task)
                break
        else:
            # self.env.process(self.machines[task["machine_id"]].remove_task(task))
            self.machines[task["machine_id"]].remove_task(task)
        self.stats["finished_tasks"] += 1
