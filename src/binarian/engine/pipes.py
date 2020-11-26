class BinaryPipe:
    def __init__(self):
        self.offset = 0
        self.total = 0
        self.threshold = 1024 * 1024
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
            self.data = self.data[self.offset:]
            self.offset = 0
        return bytes(value)

class DictPipe:
    def __init__(self):
        self.offset = 0
        self.total = 0
        self.threshold = 1024 * 1024
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
            self.data = self.data[self.offset:]
            self.offset = 0
        return value
