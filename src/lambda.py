from binarian import Pipeline, NDJsonMeasure, S3Download, NDJsonChunk, NDJsonIndex, QuickSort, DataMarker, NDJsonFlush, S3Upload, S3Object, OneToOne, Serialize, Deserialize, OneToMany, MergeSort, MinMax, S3Rename, DictDebug

def quick_sort(type, name, bucket, index, tag, input, output):
    pipeline = Pipeline(name=name, steps=[
        Deserialize(),
        NDJsonMeasure(steps=lambda: [S3Download()]),
        S3Download(),
        NDJsonChunk(chunksize=1024*1024),
        NDJsonIndex(extract=lambda row: int(row[tag])),
        QuickSort(key=lambda row: row.key),
        DataMarker(key='sorting:markers', count=16),
        NDJsonFlush(),
        S3Upload(bucket=bucket, key=lambda metadata: f'{output}.tmp/{index:04}?{metadata.get("sorting:markers").queryable()}', chunksize=128*1024*1024),
        Serialize(),
    ])

    return pipeline.start(input)

def kway_merge(type, name, bucket, index, tag, input, output):
    pipeline = Pipeline(name=name, steps=[
        Deserialize(),
        OneToMany(transform=lambda item: item.split()),
        MergeSort(piecesize=16*1024*1024, key=lambda row: row.key, steps=lambda item: [
            S3Download(),
            NDJsonIndex(extract=lambda row: int(row[tag])),
        ]),
        MinMax(key='sorting:markers'),
        NDJsonFlush(),
        S3Upload(bucket=bucket, key=lambda metadata: f'{output}.out/{index:04}', chunksize=128*1024*1024),
        S3Rename(key=lambda metadata: f'{output}.out/{index:04}?{metadata.get("sorting:markers").queryable()}'),
        Serialize(),
    ])

    return pipeline.start(input)

def handler(event, context):
    if 'type' in event and event['type'] == 'quick-sort':
        return quick_sort(**event)

    if 'type' in event and event['type'] == 'kway-merge':
        return kway_merge(**event)

    return None
