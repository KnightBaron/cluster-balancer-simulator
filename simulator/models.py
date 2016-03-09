import logging
import numpy.random
from settings import TRACEFILE, SERVICE_TASK_THRESHOLD
from simulator.utils import merge_two_dicts, GZipCSVReader


class GoogleJob(object):
    """docstring for GoogleJob"""
    SCHEMA = [
        "job_id", "task_index", "time", "duration",
        "cpu_request", "memory_request", "average_cpu_rate",
        "average_canonical_memory_usage",
        "different_machines_restriction"
    ]

    def __init__(self, db):
        self.db = db

    def generator(self):
        """
        Load jobs/tasks from preprocessed Google's trace

        The algorithm assume that tasks are sorted by time, job_id, task_index
        ascending
        """
        csv = GZipCSVReader(TRACEFILE, GoogleJob.SCHEMA)

        job = None
        for entry in csv:
            if job is None:  # 1st iteration
                job = {
                    "retries": 0,
                    "job_id": long(entry["job_id"]),
                    "start_time": long(entry["time"]),
                    "tasks": [{
                        "task_index": long(entry["task_index"]),
                        "allocated_cpu": float(entry["cpu_request"]),
                        "actual_cpu": float(entry["average_cpu_rate"]),
                        "duration": long(entry["duration"]),
                        "machine_id": None,
                        "is_service": (long(entry["duration"]) > SERVICE_TASK_THRESHOLD),
                    }]
                }
            elif job["job_id"] == long(entry["job_id"]):  # Subsequence iterations of each job
                job["tasks"].append({
                    "task_index": long(entry["task_index"]),
                    "allocated_cpu": float(entry["cpu_request"]),
                    "actual_cpu": float(entry["average_cpu_rate"]),
                    "duration": long(entry["duration"]),
                    "machine_id": None,
                    "is_service": (long(entry["duration"]) > SERVICE_TASK_THRESHOLD),
                })
            else:  # Finished loading an entire job, start loading new job
                yield job
                job = {
                    "job_id": long(entry["job_id"]),
                    "start_time": long(entry["time"]),
                    "tasks": [{
                        "task_index": long(entry["task_index"]),
                        "allocated_cpu": float(entry["cpu_request"]),
                        "actual_cpu": float(entry["average_cpu_rate"]),
                        "duration": long(entry["duration"]),
                        "machine_id": None,
                        "is_service": (long(entry["duration"]) > SERVICE_TASK_THRESHOLD),
                    }]
                }

        yield job  # Last job in the trace


class TsubameJob(object):
    """docstring for TsubameJob"""
    pass


# TODO: Plan a proper job getter sorted by start_time
class DummyJob(object):
    """docstring for DummyJob"""
    def __init__(self, db):
        self.db = db
        self.min_job = 3
        self.max_job = 4
        self.min_task = 1
        self.max_task = 4
        self.min_duration = 10
        self.max_duration = 20

    # def generator(self):
    #     jobs = self.generate_jobs()
    #     for job in sorted(jobs, key=lambda j: j["start_time"]):
    #         yield job

    # def generate_jobs(self):
    def generator(self):
        start_time = 0
        for _ in range(
                numpy.random.randint(self.min_job, self.max_job)):
            yield {
                "job_id": _,
                "start_time": start_time,
                "tasks": self.generate_tasks()}
            start_time += numpy.random.poisson()

    def generate_tasks(self):
        return [  # List comprehension
            merge_two_dicts(self.generate_task(), {"task_index": _})
            for _ in range(
                numpy.random.randint(self.min_task, self.max_task))]

    def generate_task(self):
        return {
            "allocated_cpu": numpy.random.random(),
            "allocated_memory": numpy.random.random(),
            "actual_cpu": numpy.random.random(),
            "actual_memory": numpy.random.random(),
            "duration": numpy.random.randint(self.min_duration, self.max_duration), }
