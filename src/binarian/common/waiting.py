class WaitAll:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next

    def flush(self):
        while chunks := self.prev.read(size=-1):
            self.next.append(chunks)
