from boto3 import client


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
