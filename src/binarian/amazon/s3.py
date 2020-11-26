from boto3 import client
from botocore.exceptions import ClientError


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
