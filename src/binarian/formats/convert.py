from json import dumps
from .xml import XmlReader

class XmlToJson:
    def __init__(self, rowtag, chunksize=32*1024*1024, windowsize=1024*1024):
        self.rowtag = rowtag
        self.iterator = None
        self.reader = None
        self.chunksize = chunksize
        self.windowsize = windowsize
        self.input = 'binary'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        self.process(chunksize=self.chunksize, windowsize=self.windowsize)

    def flush(self):
        self.process(chunksize=0, windowsize=0)

    def process(self, chunksize, windowsize):
        if self.reader is None and self.prev.length() > chunksize:
            self.reader = XmlReader(rowtag=self.rowtag, source=self.prev)

        while self.reader is not None and self.prev.length() > chunksize:
            if data := self.reader.tick(lambda: self.prev.length() > windowsize):
                self.next.append(bytearray(dumps(data), 'utf8') + b'\n')
