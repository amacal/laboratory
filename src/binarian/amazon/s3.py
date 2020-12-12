from time import sleep
from boto3 import client
from botocore.exceptions import ClientError

def measure_object(client, bucket, key):
    return client.head_object(Bucket=bucket, Key=key)['ContentLength']

class S3Prefix(object):
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

class S3Object(object):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def ensure_measured(self):
        if not hasattr(self, 'total'):
            self.total = measure_object(client('s3'), self.bucket, self.key)

    def range(self, index, size, total):
        return S3ObjectRange(self.bucket, self.key, total, index, min(index + size, total) - 1)

    def build(self, size, start, end):
        return [self.range(index, size, end) for index in range(start, end, size)]

    def between(self, start, end):
        self.ensure_measured()
        return S3ObjectRange(self.bucket, self.key, self.total, start, end)

    def split(self, size):
        self.ensure_measured()
        return self.build(size, 0, self.total)

    def split_between(self, size, start, end):
        self.ensure_measured()
        return self.build(size, start, end)

    def __str__(self):
        return f's3://{self.bucket}/{self.key}'

class S3ObjectRange(object):
    def __init__(self, bucket, key, total, start, end):
        self.bucket = bucket
        self.key = key
        self.total = total
        self.start = start
        self.end = end

    def between(self, start, end):
        return S3ObjectRange(self.bucket, self.key, self.total, start, end)

    def __str__(self):
        return f's3://{self.bucket}/{self.key} range {self.start}:{self.end}/{self.total}'

class S3Download:
    def __init__(self, chunksize=32*1024*1024):
        self.client = client('s3')
        self.chunksize = chunksize
        self.input = 'dict'
        self.output = 'binary'

    def length(self):
        return None

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while objects := self.prev.read(size=1):
            for target in objects:
                self.download(target)

    def measure(self, target):
        size = measure_object(self.client, target.bucket, target.key)
        self.metrics.log(f'downloading {target} measured as {size} bytes')
        return size

    def download(self, target):
        offset = 0 if not hasattr(target, 'start') else target.start
        size = self.measure(target) if not hasattr(target, 'end') else target.end + 1

        while offset < size:
            offset += self.range(target, offset, size)

    def range(self, target, offset, total):
        available = min(total - offset, self.chunksize) - 1
        self.metrics.log(f'downloading range {offset}:{offset+available}')

        response = self.client.get_object(
            Range=f'bytes={offset}-{offset+available}',
            Bucket=target.bucket,
            Key=target.key
        )

        while chunk := response['Body'].read(128 * 1024):
            self.next.append(chunk)

        return available + 1

    def flush(self):
        pass

class S3Upload:
    def __init__(self, bucket, key, chunksize):
        self.chunksize = chunksize
        self.client = client('s3')
        self.bucket = bucket
        self.part = 1
        self.parts = list()
        self.upload_id = None
        self.init_key(key)
        self.input = 'binary'
        self.output = 'dict'

    def init_key(self, key):
        self.key = None
        self.keyer = key if callable(key) else lambda metadata: key

    def length(self):
        return None

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.metadata = metadata
        self.prev.subscribe(self.changed)

    def start_upload(self):
        if not self.upload_id:
            self.key = self.keyer(self.metadata)
            self.upload_id = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)['UploadId']
            self.metrics.log(f'upload started {self.key}')            

    def changed(self):
        self.start_upload()
        self.upload(size=self.chunksize)

    def flush(self):
        self.start_upload()
        self.upload()
        self.complete()

    def upload(self, size=0):
        while self.prev.length() > size:
            chunk = self.prev.read(self.chunksize)
            self.metrics.log(f'part {self.part} started; {len(chunk)} bytes')
            response = self.client.upload_part(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, PartNumber=self.part, Body=chunk)
            self.metrics.log(f'part {self.part} completed; {len(chunk)} bytes')
            self.parts.append({'ETag': response['ETag'], 'PartNumber': self.part})
            self.part += 1

    def complete(self):
        self.client.complete_multipart_upload(Bucket=self.bucket, Key=self.key, UploadId=self.upload_id, MultipartUpload={'Parts': self.parts})
        self.metrics.log(f'upload completed {self.key}')
        self.next.append([S3Object(self.bucket, self.key)])

class S3List:
    def __init__(self):
        self.client = client('s3')
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def process(self, item):
        response = self.client.list_objects_v2(Bucket=item.bucket, Prefix=item.prefix)
        self.next.append([S3Object(item.bucket, content['Key']) for content in response.get('Contents', list())])

    def changed(self):
        while items := self.prev.read(size=-1):
            for item in items:
                self.process(item)

    def flush(self):
        self.changed()

class S3Delete:
    def __init__(self):
        self.client = client('s3')
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def process(self, items):
        objects = [{ 'Key': item.key } for item in items]
        self.client.delete_objects(Bucket=items[0].bucket, Delete={ 'Objects': objects })

    def changed(self):
        while items := self.prev.read(size=-1):
            self.process(items)
            self.next.append(items)

    def flush(self):
        self.changed()

class S3Rename:
    def __init__(self, key):
        self.key = key
        self.client = client('s3')
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.metadata = metadata
        self.prev.subscribe(self.changed)

    def process(self, items):
        result = list()
        for item in items:
            result.append(S3Object(item.bucket, self.key(self.metadata)))
            self.metrics.log(f'copying {item.bucket}/{item.key} to {result[-1].key}')
            self.client.copy_object(CopySource=f'{item.bucket}/{item.key}', Bucket=item.bucket, Key=result[-1].key)
            self.client.delete_object(Bucket=item.bucket, Key=item.key)
        return result

    def changed(self):
        while items := self.prev.read(size=-1):
            self.next.append(self.process(items))

    def flush(self):
        self.changed()

class S3Chunk:
    def __init__(self, chunksize):
        self.chunksize = chunksize
        self.client = client('s3')
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.prev.subscribe(self.changed)

    def changed(self):
        while items := self.prev.read(size=-1):
            for item in items:
                self.next.append(item.split(self.chunksize))

    def flush(self):
        self.changed()

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
