import simpy
import numpy
from simpy.resources.store import FilterStore


# TODO: Remove this if unused...
class RandomAccessFilterStore(FilterStore):
    """docstring for RandomAccessFilterStore"""
    def _do_get(self, event):
        for item in numpy.random.permutation(self.items):
            if event.filter(item):
                self.items.remove(item)
                event.succeed(item)
                break
        return True


class Machine(object):
    """docstring for Machine"""
    def __init__(self, env, cpu, memory):
        self.cpu = cpu
        self.memory = memory
        self.allocated_cpu = simpy.Container(env, init=0.0, capacity=cpu)
        self.allocated_memory = simpy.Container(env, init=0.0, capacity=memory)
        self.actual_cpu = simpy.Container(env, init=0.0, capacity=cpu)
        self.actual_memory = simpy.Container(env, init=0.0, capacity=memory)

    def is_fit(self, task):
        """Task is a dict"""
        return (
            ((self.cpu - self.allocated_cpu.level) > task["allocated_cpu"]) and
            ((self.memory - self.allocated_memory.level) > task["allocated_memory"]) and
            ((self.cpu - self.actual_cpu.level) > task["actual_cpu"]) and
            ((self.memory - self.actual_memory.level) > task["actual_memory"]))
