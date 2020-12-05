from lxml.etree import iterparse

class XmlReader:
    def __init__(self, rowtag, source):
        self.iterator = iterparse(source=source, events=['start', 'end'])
        self.rowtag = rowtag
        self.container = None
        self.path = list()
        self.previous = list()

    def length(self):
        return None

    def clean(self, node):
        while node.getprevious() is not None:
            del node.getparent()[0]

    def tick(self, condition):
        while condition():
            try:
                event, node = next(self.iterator)
                _, _, tag = node.tag.rpartition('}')
            except StopIteration:
                return None

            if self.container is None and tag == self.rowtag and event == 'start':
                self.container = dict()
                self.path.append(self.container)
                self.clean(node)
            elif self.container is not None and tag == self.rowtag and event == 'end':
                data = self.container
                self.container = None
                self.previous = list()
                self.path = list()
                return data
            elif self.container is None:
                if event == 'end':
                    self.clean(node)
            elif event == 'start':
                if self.path[-1] is None:
                    self.path[-1] = dict()
                self.previous.append(tag)
                self.path.append(None)
            elif self.path[-1] is None:
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
