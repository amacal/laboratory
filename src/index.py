from boto3 import client
from botocore.exceptions import ClientError
from datetime import datetime
from ftplib import FTP
from gzip import GzipFile
from os.path import splitext
from os import getenv
from time import sleep, time as now
from hashlib import md5, sha1
from queue import Queue
from lxml.etree import iterparse
from json import dumps
from re import compile, sub
from asyncio import get_running_loop, wait, run
from concurrent.futures import ThreadPoolExecutor


class Parameters:
    def __init__(self):
        self.ssmClient = client('ssm')

    def value(self, name):
        return self.ssmClient.get_parameter(Name=name)['Parameter']['Value']

class Metrics:
    def __init__(self, name):
        self.name = name

    def log(self, data):
        print(f'{datetime.now().strftime("%H:%M:%S")} {self.name}: {data}\n', end='')

class Metadata:
    def __init__(self):
        self.data = dict()

    def keys(self):
        return self.data.keys()

    def set(self, name, value):
        self.data[name] = value

    def get(self, name):
        return self.data[name]

class Funnel:
    def __init__(self, steps):
        self.steps = steps

    def bind(self, prev, next, metrics, metadata):
        prev = DictPipe()
        self.last = self.first = prev
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, metrics, metadata)
            self.last = prev = next

    def read(self, size=-1):
        return self.last.read(size)

    def subscribe(self, callback):
        self.last.subscribe(callback)

    def append(self, chunk):
        self.first.append(chunk)

class Pipeline:
    def __init__(self, name, steps, metrics=None, metadata=None):
        self.steps = steps
        self.metadata = Metadata() if metadata is None else metadata 
        self.metrics = Metrics(name) if metrics is None else metrics

    def init(self):
        prev = DictPipe()
        self.pipe = prev
        for step in self.steps:
            next = BinaryPipe() if step.output == 'binary' else DictPipe()
            step.bind(prev, next, self.metrics, self.metadata)
            prev = next

    def flush(self):
        for step in self.steps:
            step.flush()

    def run(self, input):
        self.pipe.append([input])

    def complete(self):
        for key in self.metadata.keys():
            self.metrics.log(f'{key} -> {self.metadata.get(key)}')

    def start(self, input=None):
        self.init()
        self.run(input)
        self.flush()
        self.complete()

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

class Ungzip:
    def __init__(self):
        self.input = 'binary'
        self.output = 'binary'

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

class XmlReader:
    def __init__(self, rowtag, iterator):
        self.iterator = iterator
        self.rowtag = rowtag
        self.container = None
        self.item = None
        self.previous = []
        self.path = []

    def tick(self, condition):
        while condition():
            try:
                event, node = next(self.iterator)
                _, _, node.tag = node.tag.rpartition('}')

                if event == 'start' and node.tag == self.rowtag:
                    self.container = dict()
                    self.path.append(self.container)
                    while node.getprevious() is not None:
                        del node.getparent()[0]
                elif event == 'end' and node.tag == self.rowtag:
                    data = bytearray(dumps(self.container), 'utf8')
                    self.container = None
                    self.path = []
                    self.previous = []
                    return data + b'\n'
                elif self.container is None:
                    pass
                elif event == 'start':
                    if self.path[-1] is None:
                        self.path[-1] = dict()
                    self.previous.append(node.tag)
                    self.path.append(None)
                elif event == 'end' and self.path[-1] is None:
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
            except StopIteration:
                return None

class XmlToJson:
    def __init__(self, rowtag, chunksize=32*1024*1024):
        self.rowtag = rowtag
        self.iterator = None
        self.reader = None
        self.chunksize = chunksize
        self.windowsize = 1024 * 1024
        self.input = 'binary'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        self.process(chunksize=self.chunksize, windowsize=self.windowsize)

    def flush(self):
        self.process(chunksize=0, windowsize=0)

    def process(self, chunksize, windowsize):
        if self.iterator is None and self.prev.length() > chunksize:
            self.iterator = iterparse(source=self.prev, events=['start', 'end'])
            self.reader = XmlReader(rowtag=self.rowtag, iterator=self.iterator)

        while self.reader is not None and self.prev.length() > chunksize:
            if data := self.reader.tick(lambda: self.prev.length() > windowsize):
                self.next.append(data)

class S3KeyExists:
    def __init__(self, bucket, key=None):
        self.client = client('s3')
        self.bucket = bucket
        self.key = key

    def get_value(self, value):
        if self.key is None or not callable(self.key):
            return value
        else:
            return self.key(value)

    def evaluate(self, value):
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=self.get_value(value)
            ) 
        except ClientError as ex:
            if ex.response['Error']['Code'] == '404':
                return False
            else:
                raise

        return True

class S3Upload:
    def __init__(self, bucket, key, chunksize):
        self.chunksize = chunksize
        self.client = client('s3')
        self.bucket = bucket
        self.key = key
        self.part = 1
        self.parts = list()
        self.upload_id = None
        self.input = 'binary'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def start_upload(self):
        if not self.upload_id:
            self.upload_id = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)['UploadId']
            self.metrics.log(f'upload started {self.key}')            

    def changed(self):
        self.start_upload()
        self.upload(self.chunksize)

    def flush(self):
        self.start_upload()
        self.upload()
        self.complete()

    def upload(self, size = 0):
        while self.prev.length() > size:
            chunk = self.prev.read(self.chunksize)
            response = self.client.upload_part(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=self.part, Body=chunk)
            self.metrics.log(f'part {self.part} completed; {len(chunk)} bytes')
            self.parts.append({'ETag': response['ETag'], 'PartNumber': self.part})
            self.part = self.part + 1

    def complete(self):
        self.client.complete_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, MultipartUpload={'Parts': self.parts})
        self.metrics.log(f'upload completed {self.key}')

class S3Download:
    def __init__(self, bucket, chunksize=32*1024*1024):
        self.client = client('s3')
        self.chunksize = chunksize
        self.bucket = bucket
        self.input = 'dict'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while keys := self.prev.read(1):
            self.download(keys[0])

    def measure(self, key):
        response = self.client.head_object(Bucket=self.bucket, Key=key)
        self.metrics.log(f'download measured {response["ContentLength"]}')
        return response['ContentLength']

    def download(self, key):
        offset = 0
        size = self.measure(key)

        while offset < size:
            offset += self.range(key, offset, size)

        self.metrics.log(f'download completed {key}')

    def range(self, key, offset, total):
        available = min(total - offset, self.chunksize) - 1
        self.metrics.log(f'downloading offset {offset}-{offset+available}')

        response = self.client.get_object(
            Range=f'bytes={offset}-{offset+available}',
            Bucket=self.bucket,
            Key=key
        )

        while chunk := response['Body'].read(128 * 1024):
            self.next.append(chunk)

        return available + 1

    def flush(self):
        pass

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

class EcsTask:
    def __init__(self, cluster, task, securityGroup, vpcSubnet, environment):
        self.cluster = cluster
        self.task = task
        self.securityGroup = securityGroup
        self.vpcSubnet = vpcSubnet
        self.environment = environment
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.ecs = client('ecs')
        self.prev.subscribe(self.changed)

    def changed(self):
        while token := self.prev.read(size=1):
            taskArn = self.start(token[0])
            self.wait(taskArn)
            token[0].release()
            self.next.append([token[0].value])

    def flush(self):
        pass

    def start(self, token):
        response = self.ecs.run_task(
            cluster=self.cluster,
            taskDefinition=self.task,
            platformVersion='1.4.0',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'assignPublicIp': 'ENABLED',
                    'securityGroups': [self.securityGroup],
                    'subnets': [self.vpcSubnet]
                }
            },
            overrides={
                'containerOverrides': [{
                    'name': self.task.replace('/', ':').split(':')[-2],
                    'environment': self.environment(token)
                }]
            }
        )

        return response["tasks"][0]["taskArn"]

    def wait(self, taskArn):
        self.metrics.log(f'waiting {taskArn}')
        self.ecs.get_waiter('tasks_stopped').wait(
            cluster=self.cluster,
            tasks=[taskArn],
            WaiterConfig={
                'Delay': 6,
                'MaxAttempts': 900
            }
        )

class Token:
    def __init__(self, metrics, queue, item, value):
        self.queue = queue
        self.item = item
        self.value = value
        self.metrics = metrics

    def release(self):
        self.queue.put(self.item)
        self.metrics.log(f'released {self.item}')

class Throttling:
    def __init__(self, queue):
        self.queue = queue
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while value := self.prev.read(size=1):
            item = self.queue.get(timeout=3600)
            token = Token(metrics=self.metrics, queue=self.queue, item=item, value=value[0])
            self.metrics.log(f'acquired {token.item}')
            self.next.append([token])

    def flush(self):
        pass

class Conditional:
    def __init__(self, condition, steps=[], inverse=False):
        self.condition = condition.evaluate
        self.inverse = inverse
        self.funnel = Funnel(steps)
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.funnel.bind(prev, next, metrics, metadata)
        self.funnel.subscribe(self.complete)
        self.prev.subscribe(self.changed)

    def satisfies(self, value):
        return self.condition(value) if not self.inverse else not self.condition(value)

    def complete(self):
        while value := self.funnel.read(size=1):
            self.next.append(value)

    def changed(self):
        while value := self.prev.read(size=1):
            if not self.satisfies(value[0]):
                self.next.append(value)
            else:
                self.funnel.append(value)

    def flush(self):
        pass

def split_name(name):
    rows = name.replace('-', '/').split('/')
    rows.insert(-1, splitext(splitext(sub('[0-9]+', '', rows[-1]))[0])[0])
    return '/'.join(rows)

def master(filename, rowtag, bucket, cluster, task, securityGroup, vpcSubnet, ftpQueue, jsonQueue):
    pipeline = Pipeline(name=filename, steps=[
        Conditional(
            inverse=True,
            condition=S3KeyExists(bucket=bucket, key=lambda value: f'raw/{split_name(value)}'),
            steps=[
                Throttling(queue=ftpQueue),
                EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda token: [
                    { 'name': 'TYPE', 'value': 'worker-ftp' },
                    { 'name': 'NAME', 'value': token.value },
                    { 'name': 'BUCKET', 'value': bucket },
                    { 'name': 'INPUT', 'value': token.value },
                    { 'name': 'OUTPUT', 'value': f'raw/{split_name(token.value)}' },
                    { 'name': 'HOST', 'value': token.item['Host'] },
                    { 'name': 'DIRECTORY', 'value': token.item['Directory'] },
                ]),
            ]
        ), 
        Conditional(
            inverse=True,
            condition=S3KeyExists(bucket=bucket, key=lambda value: f'json/{split_name(splitext(splitext(value)[0])[0])}.json'),
            steps=[
                Throttling(queue=jsonQueue),
                EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda token: [
                    { 'name': 'TYPE', 'value': 'worker-json' },
                    { 'name': 'NAME', 'value': token.value },
                    { 'name': 'ROWTAG', 'value': rowtag },
                    { 'name': 'BUCKET', 'value': bucket },
                    { 'name': 'INPUT', 'value': f'raw/{split_name(token.value)}' },
                    { 'name': 'OUTPUT', 'value': f'json/{split_name(splitext(splitext(token.value)[0])[0])}.json' },
                ]),
            ]
        )
    ])

    pipeline.start(input=filename)

def worker_ftp(name, host, directory, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        FtpDownload(host=host, directory=directory),
        S3Upload(bucket=bucket, key=output, chunksize=128*1024*1024)
    ])

    pipeline.start(input=input)

def worker_json(name, rowtag, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        S3Download(bucket=bucket),
        Ungzip(),
        XmlToJson(rowtag=rowtag),
        S3Upload(bucket=bucket, key=output, chunksize=128*1024*1024)
    ])

    pipeline.start(input=input)

def worker_sort(name, tag, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        S3Download(bucket=bucket),
        NDJsonRead(chunksize=512*1024*1024),
        ForEachChunk(steps=lambda index: [
            DictionarySort(tag=tag),
            S3Upload(bucket=bucket, key=f'{output}.tmp/{index}', chunksize=128*1024*1024)
        ])
    ])

    pipeline.start(input=input)

def fetch_names():
    names = list()
    ending = compile('enwiki-20201020-stub-meta-current[0-9]{1,2}(\.xml\.gz)$')

    ftp = FTP('ftp.acc.umu.se')
    ftp.login()

    ftp.cwd('mirror/wikimedia.org/dumps/enwiki/20201020/')
    ftp.retrlines('NLST', lambda x: names.append(x))
    ftp.quit()

    return [name for name in names if ending.search(name) is not None ]

if __name__ == '__main__' and getenv('TYPE') == 'test':
    #worker_json('test', 'page', 'wikipedia-307348727739', 'raw/enwiki/20201020/stub/meta/current/current18.xml.gz', 'json/enwiki/20201020/stub/meta/current/current18.json')
    print([split_name(name) for name in fetch_names()])

if __name__ == '__main__' and getenv('TYPE') == 'worker-ftp':
    worker_ftp(getenv('NAME'), getenv('HOST'), getenv('DIRECTORY'), getenv('BUCKET'), getenv('INPUT'), getenv('OUTPUT'))

if __name__ == '__main__' and getenv('TYPE') == 'worker-json':
    worker_json(getenv('NAME'), getenv('ROWTAG'), getenv('BUCKET'), getenv('INPUT'), getenv('OUTPUT'))

if __name__ == '__main__' and getenv('TYPE') == 'master':
    parameters = Parameters()
    ftpQueue = Queue()
    jsonQueue = Queue()

    bucket = parameters.value('/wikipedia/bucket_name')
    securityGroup = parameters.value('/wikipedia/security_group')
    vpcSubnet = parameters.value('/wikipedia/vpc_subnet')
    taskArn = parameters.value('/wikipedia/task_arn')
    clusterArn = parameters.value('/wikipedia/cluster_arn')

    for i in range(15):
        jsonQueue.put({})

    for i in range(3):
        ftpQueue.put({
            'Host': 'ftpmirror.your.org',
            'Directory': 'pub/wikimedia/dumps/enwiki/20201020/'
        })

    for i in range(3):
        ftpQueue.put({
            'Host': 'ftp.acc.umu.se',
            'Directory': 'mirror/wikimedia.org/dumps/enwiki/20201020/'
        })

    for i in range(3):
        ftpQueue.put({
            'Host': 'dumps.wikimedia.your.org',
            'Directory': 'pub/wikimedia/dumps/enwiki/20201020/'
        })

    async def main():
        tasks = []
        loop = get_running_loop()

        with ThreadPoolExecutor(max_workers=20) as executor:
            for item in fetch_names():
                tasks.append(loop.run_in_executor(executor, master, item, 'page', bucket, clusterArn, taskArn, securityGroup, vpcSubnet, ftpQueue, jsonQueue))

            await wait(tasks)

    run(main())
