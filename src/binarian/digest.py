from hashlib import md5, sha1


class MD5Hash:
    def __init__(self, name):
        self.name = name
        self.input = 'binary'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metadata = metadata
        self.prev.subscribe(self.changed)
        self.instance = md5()

    def changed(self):
        chunk = self.prev.read(-1)
        self.instance.update(chunk)
        self.next.append(chunk)

    def flush(self):
        self.metadata.set(self.name, self.instance.digest().hex())

class SHA1Hash:
    def __init__(self, name):
        self.name = name
        self.input = 'binary'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metadata = metadata
        self.prev.subscribe(self.changed)
        self.instance = sha1()

    def changed(self):
        chunk = self.prev.read(-1)
        self.instance.update(chunk)
        self.next.append(chunk)

    def flush(self):
        self.metadata.set(self.name, self.instance.digest().hex())
