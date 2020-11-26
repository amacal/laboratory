from .pipes import BinaryPipe, DictPipe


class Funnel:
    def __init__(self, steps):
        self.steps = steps

    def bind(self, prev, next, metrics, metadata):
        prev = DictPipe()
        self.last = self.first = prev
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, metrics, metadata)
            self.last = prev = next

    def read(self, size=-1):
        return self.last.read(size)

    def subscribe(self, callback):
        self.last.subscribe(callback)

    def append(self, chunk):
        self.first.append(chunk)
