class BinaryConsumer:
    def __init__(self):
        self.input = 'binary'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.prev.subscribe(self.flush)

    def flush(self):
        while self.prev.read(size=-1):
            pass

class DictConsumer:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.prev.subscribe(self.flush)

    def flush(self):
        while self.prev.read(size=-1):
            pass
