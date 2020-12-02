from time import sleep
from boto3 import client
from botocore.config import Config

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'adaptive'
   }
)

ecs = client('ecs', config=config)
logs = client('logs', config=config)

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
        self.prev.subscribe(self.changed)

    def changed(self):
        while items := self.prev.read(size=1):
            for item in items:
                self.wait(*self.start(item))
                self.next.append([item])

    def flush(self):
        pass

    def start(self, token):
        response = ecs.run_task(
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

        return (
            response['tasks'][0]['taskArn'],
            self.describe_log_options(response['tasks'][0]['taskDefinitionArn'])
        )

    def describe_log_options(self, taskDefinitionArn):
        return ecs.describe_task_definition(taskDefinition=taskDefinitionArn)['taskDefinition']['containerDefinitions'][0]['logConfiguration']['options']

    def wait(self, taskArn, logOptions):
        self.metrics.log(f'waiting {taskArn}')

        stoppedAt = None
        logArgs = {
            'startFromHead': True,
            'logGroupName': logOptions['awslogs-group'],
            'logStreamName': '/'.join([logOptions['awslogs-stream-prefix']]+taskArn.split('/')[-2:])
        }

        while not stoppedAt:
            response = ecs.describe_tasks(
                cluster=self.cluster,
                tasks=[taskArn],
            )

            if 'stoppedAt' in response['tasks'][0]:
                stoppedAt = response['tasks'][0]['stoppedAt']

            try:
                response = logs.get_log_events(**logArgs)
                logArgs['nextToken'] = response['nextForwardToken']

                for event in response['events']:
                    self.metrics.raw(event['message'])
            except logs.exceptions.ResourceNotFoundException:
                pass

            sleep(1)