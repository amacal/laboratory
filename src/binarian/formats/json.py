from orjson import loads, JSONDecodeError

class NDJsonChunk:
    def __init__(self, chunksize=1024*1024):
        self.chunksize = chunksize
        self.input = 'binary'
        self.output = 'binary'
    
    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        if self.prev.length() > self.chunksize:
            if (index := self.prev.rfind(b'\n')) > -1:
                chunk = self.prev.read(size=index+1)
                self.next.append(chunk)

    def flush(self):
        if self.prev.length() > 0:
            self.next.append(self.prev.read(size=-1))

class NDJsonIndexed:
    def __init__(self, key, data):
        self.key = key
        self.data = data

    def __str__(self):
        return f'{self.key}:{len(self.data)}'

class NDJsonIndex:
    def __init__(self, extract, chunksize=1024*1024):
        self.chunksize = chunksize
        self.extract = extract
        self.input = 'binary'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def process(self, size):
        if self.prev.length() > size:
            chunks = []
            while (index := self.prev.find(b'\n')) > -1:
                chunk = self.prev.read(size=index+1)
                key = self.extract_key(chunk)
                chunks.append(NDJsonIndexed(key, chunk))
            if len(chunks) > 0:
                self.next.append(chunks)

    def extract_key(self, chunk):
        try:
            return self.extract(loads(chunk))
        except JSONDecodeError:
            self.metrics.log(f'JSON malformed: {chunk}')
            raise

    def changed(self):
        self.process(size=self.chunksize)

    def flush(self):
        self.process(size=0)

class NDJsonFlush:
    def __init__(self):
        self.input = 'dict'
        self.output = 'binary'
        self.processed = 0

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        while chunks := self.prev.read(size=-1):
            for chunk in chunks:
                self.next.append(chunk.data)

    def flush(self):
        self.changed()
