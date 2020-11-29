class Singleton:
    def __init__(self, value):
        self.value = value
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.next = next

    def flush(self):
        self.next.append([self.value])
