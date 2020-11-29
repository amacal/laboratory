from ..engine import Funnel, BinaryPipe

class ForEachChunk:
    def __init__(self, chunksize=1024*1024, steps=[]):
        self.iteration = 0
        self.chunksize = chunksize
        self.processed = 0
        self.funnel = None
        self.init_steps(steps)
        self.input = 'binary'
        self.output = 'dict'

    def init_steps(self, steps):
        self.steps = steps if callable(steps) else lambda _: steps

    def init_funnel(self):
        if self.funnel is None:
            self.funnel = Funnel(self.steps(self.iteration))
            self.funnel.bind(self.metrics, self.metadata, prev=BinaryPipe())
            self.funnel.subscribe(self.completed)

    def close_funnel(self):
        if self.funnel is not None:
            self.funnel.flush()
            self.iteration += 1
            self.funnel = None
            self.processed = 0

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.metadata = metadata
        self.prev.subscribe(self.changed)

    def completed(self):
        while value := self.funnel.read(size=1):
            self.next.append(value)

    def changed(self):
        if self.prev.length() > 0:
            self.init_funnel()
            self.process()

    def flush(self):
        self.process()
        self.close_funnel()

    def process(self):
        if chunk := self.prev.read(size=-1):
            self.processed += len(chunk)
            self.funnel.append(chunk)

        if self.processed >= self.chunksize:
            self.close_funnel()
