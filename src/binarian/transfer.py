from ftplib import FTP
from time import time as now


class FtpDownload:
    def __init__(self, host, directory):
        self.client = None
        self.host = host
        self.directory = directory
        self.tick = int(now())
        self.input = 'dict'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.prev.subscribe(self.changed)
        self.next = next
        self.metrics = metrics

    def changed(self):
        if item := self.prev.read(1):
            self.start_client()
            self.start_download(item[0])

    def request_item(self):
        self.metrics.log(f'using {self.host}')

    def start_client(self):
        self.client = FTP(self.host)
        self.client.login()
        self.client.cwd(self.directory)

    def start_download(self, input):
        self.metrics.log(f'download started {self.directory} {input}')
        self.client.retrbinary(f'RETR {input}', self.append, blocksize=128*1024)
        self.metrics.log(f'download completed {self.directory} {input}')

    def append(self, chunk):
        self.next.append(chunk)
        self.touch(tick=int(now()))

    def touch(self, tick):
        if tick - self.tick > 60:
            self.client.putcmd('NOOP')
            self.tick = tick

    def flush(self):
        self.release_client()

    def release_client(self):
        if self.client:
            self.client.quit()
