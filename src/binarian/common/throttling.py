class Token:
    def __init__(self, metrics, queue, item, value):
        self.queue = queue
        self.item = item
        self.value = value
        self.metrics = metrics

    def release(self):
        self.queue.put(self.item)
        self.metrics.log(f'released {self.item}')

class Throttling:
    def __init__(self, queue):
        self.queue = queue
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while value := self.prev.read(size=1):
            item = self.queue.get(timeout=3600)
            token = Token(metrics=self.metrics, queue=self.queue, item=item, value=value[0])
            self.metrics.log(f'acquired {token.item}')
            self.next.append([token])

    def flush(self):
        pass
