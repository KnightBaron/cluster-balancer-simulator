import simpy


class Car(object):
    """docstring for Car"""
    def __init__(self, env):
        self.env = env
        self.action = env.process(self.run())

    def run(self):
        while True:
            print "Start parking at %d" % self.env.now
            yield self.env.timeout(5)

            print "Start driving at %d" % self.env.now
            yield self.env.timeout(2)

e = simpy.Environment()
# car = Car(e)
e.run(until=15)
