class Singleton:
    def __init__(self, value):
        self.value = value
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.next = next

    def flush(self):
        self.next.append([self.value])

class OneToOne:
    def __init__(self, transform):
        self.transform = transform
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.next = next
        self.prev = prev
        self.prev.subscribe(self.changed)
    
    def changed(self):
        while data := self.prev.read(size=-1):
            print(data)
            self.next.append([self.transform(item) for item in data])

    def flush(self):
        self.changed()

class OneToMany:
    def __init__(self, transform=lambda x: x):
        self.transform = transform
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.next = next
        self.prev = prev
        self.prev.subscribe(self.changed)
    
    def changed(self):
        while data := self.prev.read(size=1):
            for item in data:
                self.next.append(self.transform(item))

    def flush(self):
        self.changed()
