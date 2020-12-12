from orjson import dumps, loads
from boto3 import client
from botocore.config import Config

class Lambda:
    def __init__(self, function, parameters):
        self.function = function
        self.parameters = parameters
        self.input = 'dict'
        self.output = 'dict'

    def bind(self, prev, next, metrics, metadata):
        self.prev = prev
        self.next = next
        self.metrics = metrics
        self.prev.subscribe(self.changed)

    def changed(self):
        while objects := self.prev.read(size=-1):
            for item in objects:
                self.start(item)

    def flush(self):
        self.changed()

    def start(self, item):
        self.metrics.log('calling lambda function ...')

        config = Config(connect_timeout=30, read_timeout=900)
        response = client('lambda', config=config).invoke(
            FunctionName=self.function,
            InvocationType='RequestResponse',
            Payload=dumps(self.parameters(item))
        )

        self.metrics.log(f'calling lambda function completed {response["StatusCode"]}')
        data = loads(response['Payload'].read()) if 'Payload' in response else {}

        if 'errorMessage' in data:
            self.metrics.log(data, response)
            raise data

        if int(response["StatusCode"]) == 200:
            self.next.append([data])
        else:
            self.metrics.log(data, response)
