from boto3 import client
from botocore.exceptions import ClientError

def measure_object(client, bucket, key):
    return client.head_object(Bucket=bucket, Key=key)['ContentLength']

class S3Prefix:
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

class S3Object:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def split(self, size):
        total = measure_object(client('s3'), self.bucket, self.key)
        index = 0
        pieces = []

        while index < total:
            pieces.append(S3ObjectRange(self.bucket, self.key, total, index, min(index + size, total) - 1))
            index += size

        return pieces

    def __str__(self):
        return f's3://{self.bucket}/{self.key}'

class S3ObjectRange:
    def __init__(self, bucket, key, total, start, end):
        self.bucket = bucket
        self.key = key
        self.total = total
        self.start = start
        self.end = end

    def __str__(self):
        return f's3://{self.bucket}/{self.key} range {self.start}:{self.end}/{self.total}'

class S3Download:
    def __init__(self, chunksize=32*1024*1024):
        self.client = client('s3')
        self.chunksize = chunksize
        self.input = 'dict'
        self.output = 'binary'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while objects := self.prev.read(1):
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

        self.metrics.log(f'downloading completed {target}')

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
        self.key = key
        self.part = 1
        self.parts = list()
        self.upload_id = None
        self.input = 'binary'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def start_upload(self):
        if not self.upload_id:
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
