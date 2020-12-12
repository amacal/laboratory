from heapq import heappush, heappop, heapify
from ..engine import Funnel, BinaryPipe, DictPipe

class MinMax:
    def __init__(self, key):
        self.key = key
        self.offset = 0
        self.min = None
        self.min_offset = None
        self.max = None
        self.max_offset = None
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metadata = metadata
        self.prev.subscribe(self.changed)

    def apply(self, data):
        for item in data:
            if self.min is None or self.min > item.key:
                self.min = item.key
                self.min_offset = self.offset
            self.offset += len(item.data)
            if self.max is None or self.max < item.key:
                self.max = item.key
                self.max_offset = self.offset

    def changed(self):
        while data := self.prev.read(size=-1):
            self.apply(data)
            self.next.append(data)

    def flush(self):
        self.changed()
        self.push()

    def push(self):
        self.metadata.set(self.key, DataMarkerCollection({
            str(self.min_offset): str(self.min),
            str(self.max_offset): str(self.max),
        }))

class DataMarkerCollection(object):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return str(self.data)

    def queryable(self):
        return '&'.join([f'{key}={value}' for key, value in self.data.items()])

class DataMarker:
    def __init__(self, key, count):
        self.key = key
        self.count = count
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metadata = metadata

    def index(self, index, data):
        return index * len(data) // self.count

    def datapoints(self, data):
        return [self.index(i, data) for i in list(range(self.count))] + [len(data)-1]

    def measure(self, data, index):
        if index == len(data)-1:
            index += 1
        return sum([len(data[i].data) for i in range(index)])

    def collection(self, data):
        return DataMarkerCollection({ str(self.measure(data, i)): data[i].key for i in self.datapoints(data) })

    def markdown(self, data):
        self.metadata.set(self.key, self.collection(data))

    def flush(self):
        if data := self.prev.read(size=-1):
            self.markdown(data)
            self.next.append(data)

class MergeGroupCollection(object):
    def __init__(self, values, objects):
        self.values = values
        self.objects = objects

    def __str__(self):
        return f'{self.values[0]}:{self.values[1]} in [{",".join(map(str, self.objects))}]'

    def split(self):
        return [MergeGroupRestricted(self, item) for item in self.objects]

class MergeGroupObject(object):
    def __init__(self, tuple, inclusive):
        self.target = tuple[4]
        self.inclusive = inclusive
        self.values = (tuple[0], tuple[1])
        self.ranges = (tuple[2], tuple[3])

    def split(self, piecesize):
        return self.target.split_between(piecesize, self.ranges[0], self.ranges[1])

    def extend(self, end, inclusive):
        self.ranges = (self.ranges[0], end)
        self.inclusive = (self.inclusive[0], inclusive)

    def __str__(self):
        return f'{self.values[0]}:{self.values[1]}/{self.ranges[0]}:{self.ranges[1]}/{self.target.key.split("?")[-2].split("/")[-1]}'

class MergeGroupRestricted(object):
    def __init__(self, collection, data):
        self.offset = data.ranges[0]
        self.inclusive = data.inclusive
        self.start = collection.values[0]
        self.end = collection.values[1]
        self.split_call = data.split

    def split(self, piecesize):
        return self.split_call(piecesize)

    def filter(self, key, index):
        return (self.start<key or self.start==key and (self.inclusive[0] or self.offset==0 or index==0)) and (key<self.end or key==self.end and self.inclusive[1])

class MergeGroup:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metadata = metadata

    def consolidate(self, items):
        output = list()
        current = None
        for item in items:
            if current is not None and current.target == item.target and current.ranges[1] == item.ranges[0]:
                current.extend(item.ranges[1], item.inclusive[1])
            else:
                current = item
                output.append(item)

        return output

    def flush(self):
        xaxis = set()
        offsets = list()
        if data := self.prev.read(size=-1):
            for entry in data:
                pairs = [item.split('=') for item in entry.key.split('?')[-1].split('&')]
                xaxis.update([int(value) for key, value in pairs])
                offsets.extend([(int(pairs[i][1]), int(pairs[i+1][1]), int(pairs[i][0]), int(pairs[i+1][0]), entry, i+1==len(pairs)-1) for i in range(len(pairs)-1)])

        hop = len(data)
        xaxis = sorted(xaxis)
        for index in range(len(xaxis)-1):
            if index % hop == 0:
                self.next.append([MergeGroupCollection((xaxis[index], xaxis[min(index+hop,len(xaxis)-1)]), self.consolidate([MergeGroupObject(offset, (xaxis[index]!=offset[0], xaxis[min(index+hop,len(xaxis)-1)]==offset[1] or offset[5])) for offset in offsets if (not offset[5] or offset[1]>xaxis[index]) and xaxis[index]<=offset[1] and offset[0]<xaxis[min(index+hop,len(xaxis)-1)]]))])

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

    def filter(self, key, index):
        return True

    def flush(self):
        heads = []
        data = self.prev.read(size=-1)
        indices = [0 for item in data]
        funnels = [Funnel(self.steps(item)) for item in data]
        pieces = [item.split(self.piecesize) for item in data]
        filters = [item.filter if hasattr(item, 'filter') else self.filter for item in data]

        for funnel in funnels:
            funnel.bind(self.metrics, self.metadata)

        def push(i):
            while funnel := funnels[i]:
                if item := funnel.read(size=1):
                    if filters[i](item[0].key, indices[i]):
                        indices[i] += 1
                        heappush(heads, (item[0].key, i, item[0]))
                        break
                elif len(pieces[i]) > 0:
                    piece = pieces[i].pop(0)
                    funnel.append([piece])
                    push(i)
                    break
                else:
                    funnel.flush()
                    funnels[i] = None
                    while item := funnel.read(size=1):
                        if filters[i](item[0].key, indices[i]):
                            indices[i] += 1
                            heappush(heads, (item[0].key, i, item[0]))
                    break

        for i in range(len(data)):
            push(i)

        heapify(heads)
        while len(heads) > 0:
            item = heappop(heads)
            self.next.append([item[2]])
            push(item[1])
