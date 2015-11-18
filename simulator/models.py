import numpy.random
from simulator.utils import merge_two_dicts


class GoogleJob(object):
    """docstring for GoogleJob"""
    def __init__(self, db):
        self.db = db
        self.jobs = self._load_jobs()

    def _load_jobs(self):
        # TODO: WRITE ME
        return []

    def generator(self):
        return iter(self.jobs)


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
