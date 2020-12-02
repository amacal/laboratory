from gzip import GzipFile

class Ungzip:
    def __init__(self):
        self.input = 'binary'
        self.output = 'binary'

    def length(self):
        return None

    def bind(self, prev, next, metrics, metadata):
        self.next = next
        self.prev = prev
        self.prev.subscribe(self.changed)
        self.file = GzipFile(mode='r', fileobj=prev)

    def changed(self):
        while self.prev.length() > 1024 * 1024:
            self.next.append(self.file.read(128 * 1024))

    def flush(self):
        while True:
            chunk = self.file.read(128 * 1024)
            if len(chunk) == 0:
                break
            self.next.append(chunk)