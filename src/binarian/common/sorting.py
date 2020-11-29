from heapq import heappush, heappop, heapify
from ..engine import Funnel, BinaryPipe, DictPipe

class QuickSort:
    def __init__(self, key):
        self.key = key
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next

    def flush(self):
        if data := self.prev.read(size=-1):
            data.sort(key=self.key)
            self.next.append(data)

class MergeSort:
    def __init__(self, piecesize=4*1024*1024, key=lambda x: x, steps=[]):
        self.key = key
        self.piecesize = piecesize
        self.steps = steps
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.metadata = metadata

    def flush(self):
        heads = []
        data = self.prev.read(size=-1)
        funnels = [Funnel(self.steps(item)) for item in data]
        pieces = [item.split(size=self.piecesize) for item in data]

        for funnel in funnels:
            funnel.bind(self.metrics, self.metadata)

        def push(i):
            if funnel := funnels[i]:
                if item := funnel.read(size=1):
                    heappush(heads, (item[0].key, i, item[0]))
                elif len(pieces[i]) > 0:
                    piece = pieces[i].pop(0)
                    funnel.append([piece])
                    push(i)
                else:
                    funnel.flush()
                    funnels[i] = None
                    while item := funnel.read(size=1):
                        heappush(heads, (item[0].key, i, item[0]))

        for i in range(len(data)):
            push(i)

        heapify(heads)
        while len(heads) > 0:
            item = heappop(heads)
            self.next.append([item[2]])
            push(item[1])
