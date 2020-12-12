from dill import dumps, loads
from base64 import b64encode, b64decode

class Serialize:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        while items := self.prev.read(size=1):
            self.next.append([b64encode(dumps(item)).decode('ascii') for item in items])

    def flush(self):
        self.changed()

class Deserialize:
    def __init__(self):
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        while items := self.prev.read(size=1):
            self.next.append([loads(b64decode(item.encode('ascii'))) for item in items])

    def flush(self):
        self.changed()
