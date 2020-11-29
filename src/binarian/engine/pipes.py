class BinaryPipe:
    def __init__(self, threshold=1024*1024):
        self.offset = 0
        self.total = 0
        self.threshold = threshold
        self.data = bytearray()
        self.callback = None

    def append(self, chunk):
        self.data += chunk
        self.callback() if self.callback is not None else None

    def subscribe(self, callback):
        self.callback = callback

    def length(self):
        return len(self.data) - self.offset

    def read(self, size):
        size = size if size >= 0 else self.length()
        value = self.data[self.offset:self.offset+size]
        self.offset = self.offset + len(value)
        if self.offset >= self.threshold:
            self.data = self.data[self.offset:] if len(self.data) > self.offset else bytearray()
            self.offset = 0
        return bytes(value)

    def rfind(self, sub):
        return self.data.rfind(sub, self.offset, len(self.data)) - self.offset

    def find(self, sub):
        return self.data.find(sub, self.offset, len(self.data)) - self.offset

class DictPipe:
    def __init__(self, threshold = 64):
        self.offset = 0
        self.total = 0
        self.threshold = threshold
        self.data = list()
        self.callback = None

    def append(self, chunk):
        self.data += chunk
        self.callback() if self.callback is not None else None

    def subscribe(self, callback):
        self.callback = callback

    def length(self):
        return len(self.data) - self.offset

    def read(self, size):
        size = size if size >= 0 else self.length()
        value = self.data[self.offset:self.offset+size]
        self.offset = self.offset + len(value)
        if self.offset >= self.threshold:
            self.data = self.data[self.offset:] if len(self.data) > self.offset else list()
            self.offset = 0
        return value
