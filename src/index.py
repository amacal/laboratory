from boto3 import client
from ftplib import FTP
from os.path import splitext
from os import getenv
from queue import Queue
from re import compile, sub
from asyncio import get_running_loop, wait, run
from concurrent.futures import ThreadPoolExecutor

from binarian import Pipeline, Singleton, OneToMany, OneToOne, S3Prefix, S3Object, S3List, S3Delete, S3Download, S3Upload, S3Rename, S3Chunk, Ungzip, XmlToJson, Conditional, ForEachChunk, ForEachItem, ForEachItemParallel, S3KeyExists, AcquireToken, ReleaseToken, QuickSort, MergeSort, DataMarker, MergeGroup, MinMax, DictDebug, BinaryDebug, WaitAll, EcsTask, Lambda, FtpDownload, NDJsonChunk, NDJsonIndex, NDJsonFlush, NDJsonMeasure, DictConsumer, BinaryConsumer, Serialize, Deserialize

class Parameters:
    def __init__(self):
        self.ssmClient = client('ssm')

    def value(self, name):
        return self.ssmClient.get_parameter(Name=name)['Parameter']['Value']

def split_name(name):
    rows = name.replace('-', '/').split('/')
    rows.insert(-1, splitext(splitext(sub('[0-9]+', '', rows[-1]))[0])[0])
    return '/'.join(rows)

def master_get(filename, rowtag, bucket, cluster, task, securityGroup, vpcSubnet, ftpQueue, jsonQueue):
    pipeline = Pipeline(name=filename, steps=[
        Conditional(
            inverse=True,
            condition=S3KeyExists(bucket=bucket, key=lambda value: f'raw/{split_name(value)}'),
            steps=[
                AcquireToken(queue=ftpQueue),
                EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda token: [
                    { 'name': 'TYPE', 'value': 'worker-ftp' },
                    { 'name': 'NAME', 'value': token.value },
                    { 'name': 'BUCKET', 'value': bucket },
                    { 'name': 'INPUT', 'value': token.value },
                    { 'name': 'OUTPUT', 'value': f'raw/{split_name(token.value)}' },
                    { 'name': 'HOST', 'value': token.item['Host'] },
                    { 'name': 'DIRECTORY', 'value': token.item['Directory'] },
                ]),
                ReleaseToken(queue=ftpQueue),
            ]
        ), 
        Conditional(
            inverse=True,
            condition=S3KeyExists(bucket=bucket, key=lambda value: f'json/{split_name(splitext(splitext(value)[0])[0])}.json'),
            steps=[
                AcquireToken(queue=jsonQueue),
                EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda token: [
                    { 'name': 'TYPE', 'value': 'worker-json' },
                    { 'name': 'NAME', 'value': token.value },
                    { 'name': 'ROWTAG', 'value': rowtag },
                    { 'name': 'BUCKET', 'value': bucket },
                    { 'name': 'INPUT', 'value': f'raw/{split_name(token.value)}' },
                    { 'name': 'OUTPUT', 'value': f'json/{split_name(splitext(splitext(token.value)[0])[0])}.json' },
                ]),
                ReleaseToken(queue=jsonQueue),
            ]
        )
    ])

    pipeline.start(input=filename)

def master_sort(filename, tag, bucket, cluster, task, securityGroup, vpcSubnet):
    pipeline = Pipeline(name=filename, steps=[
        Conditional(
            inverse=True,
            condition=S3KeyExists(bucket=bucket, key=lambda value: f'sort/{split_name(value)}'),
            steps=[
                EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda value: [
                    { 'name': 'TYPE', 'value': 'worker-sort' },
                    { 'name': 'NAME', 'value': value },
                    { 'name': 'TAG', 'value': tag },
                    { 'name': 'BUCKET', 'value': bucket },
                    { 'name': 'INPUT', 'value': f'json/{split_name(value)}' },
                    { 'name': 'OUTPUT', 'value': f'sort/{split_name(value)}' },
                ]),
            ]
        ), 
    ])

    pipeline.start(input=filename)

def driver(cluster, task, securityGroup, vpcSubnet):
    pipeline = Pipeline(name='driver', steps=[
        EcsTask(cluster=cluster, task=task, securityGroup=securityGroup, vpcSubnet=vpcSubnet, environment=lambda value: [
            { 'name': 'TYPE', 'value': 'master' }
        ])
    ])

    pipeline.start(input=None)

def worker_ftp(name, host, directory, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        FtpDownload(host=host, directory=directory),
        S3Upload(bucket=bucket, key=output, chunksize=128*1024*1024)
    ])

    pipeline.start(input=input)

def worker_json(name, rowtag, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        S3Download(),
        Ungzip(),
        XmlToJson(rowtag=rowtag),
        S3Upload(bucket=bucket, key=output, chunksize=128*1024*1024)
    ])

    pipeline.start(input=S3Object(bucket=bucket, key=input))

def worker_sort(name, tag, bucket, input, output):
    pipeline = Pipeline(name=name, steps=[
        S3Chunk(chunksize=512*1024*1024),
        ForEachItemParallel(threads=16, steps=lambda index, metadata: [
            Serialize(),
            Lambda('wikipedia-run', lambda item: {
                "type": "quick-sort",
                "name": f'{name}-{index}',
                "bucket": bucket,
                "index": index,
                "tag": tag,
                "input": item,
                "output": output
            }),
            OneToMany(),
            Deserialize(),
        ]),
        MergeGroup(),
        ForEachItemParallel(threads=32, steps=lambda index, metadata: [
            Serialize(),
            Lambda('wikipedia-run', lambda item: {
                "type": "kway-merge",
                "name": f'{name}-{index}',
                "bucket": bucket,
                "index": index,
                "tag": tag,
                "input": item,
                "output": output
            }),
            OneToMany(),
            Deserialize(),
        ]),
        #Singleton(value=S3Prefix(bucket=bucket, prefix=f'{output}.tmp/')),
        #S3List(),
        #S3Delete(),
        WaitAll(),
        DictDebug(),
    ])

    pipeline.start(input=S3Object(bucket=bucket, key=input))

def fetch_names():
    names = list()
    ending = compile('enwiki-20201120-stub-meta-history[0-9]{1,2}(\.xml\.gz)$')

    ftp = FTP('ftp.acc.umu.se')
    ftp.login()

    ftp.cwd('mirror/wikimedia.org/dumps/enwiki/20201120/')
    ftp.retrlines('NLST', lambda x: names.append(x))
    ftp.quit()

    return [name for name in names if ending.search(name) is not None ]

if __name__ == '__main__' and getenv('TYPE') == 'test':
    parameters = Parameters()
    bucket = parameters.value('/wikipedia/bucket_name')
    securityGroup = parameters.value('/wikipedia/security_group')
    vpcSubnet = parameters.value('/wikipedia/vpc_subnet')
    taskArn = parameters.value('/wikipedia/task_arn')
    clusterArn = parameters.value('/wikipedia/cluster_arn')

    ftpQueue = Queue()
    jsonQueue = Queue()
    jsonQueue.put({})

    #worker_json('test', 'revision', bucket, 'raw/enwiki/20201120/stub/meta/history/history27.xml.gz', 'json/enwiki/20201120/stub/meta/history/history27.json')
    #master_get('enwiki-20201120-stub-meta-history27.xml.gz', 'revision', bucket, clusterArn, taskArn, securityGroup, vpcSubnet, ftpQueue, jsonQueue)
    worker_sort('test', 'id', bucket, 'json/enwiki/20201120/stub/meta/history/history24.json', 'sort/enwiki/20201120/stub/meta/history/history24.json')
    #master_sort('enwiki-20201120-stub-meta-history27.json', 'id', bucket, clusterArn, taskArn, securityGroup, vpcSubnet)
    #driver(clusterArn, taskArn, securityGroup, vpcSubnet)

if __name__ == '__main__' and getenv('TYPE') == 'worker-ftp':
    worker_ftp(getenv('NAME'), getenv('HOST'), getenv('DIRECTORY'), getenv('BUCKET'), getenv('INPUT'), getenv('OUTPUT'))

if __name__ == '__main__' and getenv('TYPE') == 'worker-json':
    worker_json(getenv('NAME'), getenv('ROWTAG'), getenv('BUCKET'), getenv('INPUT'), getenv('OUTPUT'))

if __name__ == '__main__' and getenv('TYPE') == 'worker-sort':
    worker_sort(getenv('NAME'), getenv('TAG'), getenv('BUCKET'), getenv('INPUT'), getenv('OUTPUT'))

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
            'Directory': 'pub/wikimedia/dumps/enwiki/20201120/'
        })

    for i in range(3):
        ftpQueue.put({
            'Host': 'ftp.acc.umu.se',
            'Directory': 'mirror/wikimedia.org/dumps/enwiki/20201120/'
        })

    for i in range(3):
        ftpQueue.put({
            'Host': 'dumps.wikimedia.your.org',
            'Directory': 'pub/wikimedia/dumps/enwiki/20201120/'
        })

    async def main():
        tasks = []
        loop = get_running_loop()

        with ThreadPoolExecutor(max_workers=20) as executor:
            for item in fetch_names():
                tasks.append(loop.run_in_executor(executor, master_get, item, 'revision', bucket, clusterArn, taskArn, securityGroup, vpcSubnet, ftpQueue, jsonQueue))
                #tasks.append(loop.run_in_executor(executor, master_sort, splitext(splitext(item)[0])[0]+'.json', 'title', bucket, clusterArn, taskArn, securityGroup, vpcSubnet))

            await wait(tasks)

    run(main())
