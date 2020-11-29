class DictDebug:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while chunks := self.prev.read(size=1):
            self.metrics.log(f'debug {chunks[0]}')
            self.next.append(chunks)
    
    def flush(self):
        self.changed()

class BinaryDebug:
    def __init__(self):
        self.input = 'binary'
        self.output = 'binary'
        self.total = 0

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while chunks := self.prev.read(size=-1):
            self.total += len(chunks)
            self.metrics.log(f'debug {len(chunks)}/{self.total}')
            self.next.append(chunks)
    
    def flush(self):
        self.changed()