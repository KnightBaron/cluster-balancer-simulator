import simpy
import numpy
from simpy.resources.store import FilterStore


# TODO: Remove this if unused...
# class RandomAccessFilterStore(FilterStore):
#     """docstring for RandomAccessFilterStore"""
#     def _do_get(self, event):
#         for item in numpy.random.permutation(self.items):
#             if event.filter(item):
#                 self.items.remove(item)
#                 event.succeed(item)
#                 break
#         return True


class Machine(object):
    """docstring for Machine"""
    def __init__(self, env, cpu, memory, overcommit_factor=1):
        self.cpu = cpu
        self.adjusted_cpu = cpu * overcommit_factor
        self.allocated_cpu = simpy.Container(env, init=0.0, capacity=self.adjusted_cpu)
        self.actual_cpu = simpy.Container(env, init=0.0, capacity=cpu)
        # self.memory = memory
        # self.allocated_memory = simpy.Container(env, init=0.0, capacity=memory)
        # self.actual_memory = simpy.Container(env, init=0.0, capacity=memory)

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return "Allocated: {}/{}, Actual: {}/{}".format(
            self.allocated_cpu.level, self.allocated_cpu.capacity,
            self.actual_cpu.level, self.actual_cpu.capacity
        )

    def is_fit(self, task):
        """
        Check whether task could fit

        Task is a dict
        Compare only CPU for now
        """
        return (
            ((self.cpu - self.allocated_cpu.level) > task["allocated_cpu"]) and
            ((self.cpu - self.actual_cpu.level) > task["actual_cpu"])
            # ((self.memory - self.allocated_memory.level) > task["allocated_memory"]) and
            # ((self.memory - self.actual_memory.level) > task["actual_memory"])
        )

    def add_task(self, task):
        if task["allocated_cpu"] > 0:
            yield self.allocated_cpu.put(task["allocated_cpu"])
        if task["actual_cpu"] > 0:
            yield self.actual_cpu.put(task["actual_cpu"])

    def remove_task(self, task):
        if task["allocated_cpu"] > 0:
            yield self.allocated_cpu.get(task["allocated_cpu"])
        if task["actual_cpu"] > 0:
            yield self.actual_cpu.get(task["actual_cpu"])
