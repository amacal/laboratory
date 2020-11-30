echo 'Running script'
python -um cProfile -s tottime /app/wikipedia/index.py | tee /tmp/profiler.txt
aws s3 cp /tmp/profiler.txt s3://`aws ssm get-parameter --name '/wikipedia/bucket_name' | jq -rc '.Parameter.Value'`/profile.txt