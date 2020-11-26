from .common import Metadata, Metrics
from .pipes import BinaryPipe, DictPipe


class Pipeline:
    def __init__(self, name, steps, metrics=None, metadata=None):
        self.steps = steps
        self.metadata = Metadata() if metadata is None else metadata 
        self.metrics = Metrics(name) if metrics is None else metrics

    def init(self):
        prev = DictPipe()
        self.pipe = prev
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, self.metrics, self.metadata)
            prev = next

    def flush(self):
        for step in self.steps:
            step.flush()

    def run(self, input):
        self.pipe.append([input])

    def complete(self):
        for key in self.metadata.keys():
            self.metrics.log(f'{key} -> {self.metadata.get(key)}')

    def start(self, input=None):
        self.init()
        self.run(input)
        self.flush()
        self.complete()
