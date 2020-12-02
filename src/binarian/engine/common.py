from datetime import datetime
from resource import getrusage, RUSAGE_SELF

class Metrics:
    def __init__(self, name):
        self.name = name

    def raw(self, data):
        print(f'{data}\n', end='')

    def log(self, data):
        print(f'{datetime.utcnow().strftime("%H:%M:%S")} {int(getrusage(RUSAGE_SELF).ru_maxrss / 1024):04} {self.name}: {data}\n', end='')

class Metadata:
    def __init__(self):
        self.data = dict()

    def keys(self):
        return self.data.keys()

    def set(self, name, value):
        self.data[name] = value

    def get(self, name):
        return self.data[name]
