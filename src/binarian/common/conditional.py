from ..engine import Funnel

class Conditional:
    def __init__(self, condition, steps=[], inverse=False):
        self.condition = condition.evaluate
        self.inverse = inverse
        self.funnel = Funnel(steps)
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.funnel.bind(prev, next, metrics, metadata)
        self.funnel.subscribe(self.complete)
        self.prev.subscribe(self.changed)

    def satisfies(self, value):
        return self.condition(value) if not self.inverse else not self.condition(value)

    def complete(self):
        while value := self.funnel.read(size=1):
            self.next.append(value)

    def changed(self):
        while value := self.prev.read(size=1):
            if not self.satisfies(value[0]):
                self.next.append(value)
            else:
                self.funnel.append(value)

    def flush(self):
        pass
