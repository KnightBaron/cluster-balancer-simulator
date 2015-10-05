import logging


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
            logging.debug("Submit job {} at time {}".format(job["job_id"], job["start_time"]))
            yield self.env.process(self.schedule_job(job))

    def schedule_job(self, job):
        for task in job["tasks"]:
            yield self.env.process(self.schedule_task(job["job_id"], task))

    def schedule_task(self, job_id, task):
        self.env.process(self.run_task(job_id, task))
        yield self.env.timeout(0)  # Task scheduling is done immediately!

    def run_task(self, job_id, task):
        logging.info("{} - Start job {} task {}".format(self.env.now, job_id, task["task_index"]))
        yield self.env.timeout(task["duration"])
        logging.info("{} - End job {} task {}".format(self.env.now, job_id, task["task_index"]))


class Balancer(object):
    """docstring for Balancer"""
    def __init__(self):
        pass
