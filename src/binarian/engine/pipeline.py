from .common import Metadata, Metrics
from .pipes import BinaryPipe, DictPipe

class Pipeline:
    def __init__(self, name, steps):
        self.name = name
        self.steps = steps

    def init(self, metrics, metadata):
        prev = DictPipe()
        pipes = list()
        self.pipe = prev
        pipes.append(prev)
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, metrics, metadata)
            prev = next
            pipes.append(step)
            pipes.append(prev)
        metrics.pipes = pipes
        return prev

    def flush(self):
        for step in self.steps:
            step.flush()

    def run(self, input):
        self.pipe.append([input])

    def complete(self, metrics, metadata):
        for key in metadata.keys():
            metrics.log(f'{key} -> {metadata.get(key)}')

    def start(self, input=None, metrics=None, metadata=None):
        metadata = metadata if metadata else Metadata() 
        metrics = metrics if metrics else Metrics(self.name)
        pipe = self.init(metrics, metadata)

        self.run(input)
        self.flush()
        self.complete(metrics, metadata)

        return pipe.read(size=-1)
