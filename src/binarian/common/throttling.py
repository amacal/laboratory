class Token:
    def __init__(self, item, value):
        self.item = item
        self.value = value

class AcquireToken:
    def __init__(self, queue, timeout=3600):
        self.queue = queue
        self.timeout = timeout
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while value := self.prev.read(size=1):
            item = self.queue.get(timeout=self.timeout)
            token = Token(item=item, value=value[0])
            self.metrics.log(f'acquired {token.item}')
            self.next.append([token])

    def flush(self):
        pass

class ReleaseToken:
    def __init__(self, queue):
        self.queue = queue
        self.input = 'dict'
        self.output = 'dict'
    
    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def release(self, item):
        self.queue.put(item)
        self.metrics.log(f'released {item}')

    def changed(self):
        while token := self.prev.read(size=1):
            self.release(token[0].item)
            self.next.append([token[0].value])

    def flush(self):
        pass
