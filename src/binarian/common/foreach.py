from concurrent.futures import ThreadPoolExecutor
from ..engine import Funnel, BinaryPipe, DictPipe

class ForEachItem:
    def __init__(self, steps=[]):
        self.iteration = 0
        self.funnel = None
        self.init_steps(steps)
        self.input = 'dict'
        self.output = 'dict'

    def init_steps(self, steps):
        self.steps = steps if callable(steps) else lambda index, metadata: steps

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
        self.process()

    def flush(self):
        self.process()

    def process(self):
        while chunk := self.prev.read(size=-1):
            self.init_funnel()
            self.funnel.append(chunk)
            self.close_funnel()

    def init_funnel(self):
        if self.funnel is None:
            kwards = {'index': self.iteration, 'metadata': self.metadata}
            self.funnel = Funnel(self.steps(**kwards))
            self.funnel.bind(self.metrics, self.metadata, prev=DictPipe())
            self.funnel.subscribe(self.completed)

    def close_funnel(self):
        if self.funnel is not None:
            self.funnel.flush()
            self.iteration += 1
            self.funnel = None

class ForEachItemParallel:
    def __init__(self, threads=1, steps=[]):
        self.iteration = 0
        self.funnel = None
        self.threads = threads
        self.init_steps(steps)
        self.worker = None
        self.input = 'dict'
        self.output = 'dict'

    def init_steps(self, steps):
        self.steps = steps if callable(steps) else lambda index, metadata: steps

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.metadata = metadata
        self.prev.subscribe(self.changed)

    def changed(self):
        self.init_worker()
        self.process()

    def flush(self):
        self.init_worker()
        self.process()
        self.close_worker()

    def process(self):
        while chunk := self.prev.read(size=-1):
            for item in chunk:
                self.iteration += 1
                self.add_task(item, self.iteration-1)

    def init_worker(self):
        if self.worker is None:
            self.worker = ThreadPoolExecutor(max_workers=self.threads)

    def close_worker(self):
        if self.worker is not None:
            self.worker.shutdown()
            self.worker = None

    def add_task(self, item, iteration):
        self.worker.submit(self.start_task, item, iteration)

    def start_task(self, item, iteration):
        kwards = {'index': iteration, 'metadata': self.metadata}
        funnel = Funnel(self.steps(**kwards))

        def completed():
            while value := funnel.read(size=1):
                self.next.append(value)
            
        funnel.bind(self.metrics, self.metadata, prev=DictPipe())
        funnel.subscribe(completed)
        funnel.append([item])
        funnel.flush()

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
        self.steps = steps if callable(steps) else lambda index, metadata: steps

    def init_funnel(self):
        if self.funnel is None:
            kwards = {'index': self.iteration, 'metadata': self.metadata}
            self.funnel = Funnel(self.steps(**kwards))
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
