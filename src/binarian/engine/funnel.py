from .pipes import BinaryPipe, DictPipe

class Funnel:
    def __init__(self, steps):
        self.steps = steps
        self.first = None
        self.last = None

    def bind(self, metrics, metadata, prev=None):
        prev = DictPipe() if prev is None else prev
        self.last = self.first = prev 
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, metrics, metadata)
            self.last = prev = next

    def read(self, size=-1):
        return self.last.read(size)

    def flush(self):
        for step in self.steps:
            step.flush()

    def subscribe(self, callback):
        self.last.subscribe(callback)

    def append(self, chunk):
        self.first.append(chunk)
