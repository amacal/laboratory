from lxml.etree import iterparse
from json import dumps


class XmlReader:
    def __init__(self, rowtag, iterator):
        self.iterator = iterator
        self.rowtag = rowtag
        self.container = None
        self.item = None
        self.previous = []
        self.path = []

    def tick(self, condition):
        while condition():
            try:
                event, node = next(self.iterator)
                _, _, node.tag = node.tag.rpartition('}')

                if event == 'start' and node.tag == self.rowtag:
                    self.container = dict()
                    self.path.append(self.container)
                    while node.getprevious() is not None:
                        del node.getparent()[0]
                elif event == 'end' and node.tag == self.rowtag:
                    data = bytearray(dumps(self.container), 'utf8')
                    self.container = None
                    self.path = []
                    self.previous = []
                    return data + b'\n'
                elif self.container is None:
                    pass
                elif event == 'start':
                    if self.path[-1] is None:
                        self.path[-1] = dict()
                    self.previous.append(node.tag)
                    self.path.append(None)
                elif event == 'end' and self.path[-1] is None:
                    self.path[-2][self.previous[-1]] = node.text
                    self.path.pop()
                    self.previous.pop()
                elif self.previous[-1] in self.path[-2]:
                    if isinstance(self.path[-2][self.previous[-1]], list):
                        self.path[-2][self.previous[-1]].append(self.path[-1])
                    else:
                        self.path[-2][self.previous[-1]] = [self.path[-2][self.previous[-1]], self.path[-1]]
                    self.path.pop()
                    self.previous.pop()
                else:
                    self.path[-2][self.previous[-1]] = self.path[-1]
                    self.path.pop()
                    self.previous.pop()
            except StopIteration:
                return None

class XmlToJson:
    def __init__(self, rowtag, chunksize=32*1024*1024):
        self.rowtag = rowtag
        self.iterator = None
        self.reader = None
        self.chunksize = chunksize
        self.windowsize = 1024 * 1024
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
        if self.iterator is None and self.prev.length() > chunksize:
            self.iterator = iterparse(source=self.prev, events=['start', 'end'])
            self.reader = XmlReader(rowtag=self.rowtag, iterator=self.iterator)

        while self.reader is not None and self.prev.length() > chunksize:
            if data := self.reader.tick(lambda: self.prev.length() > windowsize):
                self.next.append(data)
