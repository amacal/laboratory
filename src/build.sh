PREV=$(pwd)
rm ../tmp/* -f || true

pipenv install --ignore-pipfile --python 3.9
find . -type f | grep -Ev "v-env|Dockerfile|Makefile|Pipfile|requirements\.txt|\.env|test\.py$" | sort | zip -9 -X -r --quiet $PREV/../tmp/wikipedia-app.zip -@

cd $(pipenv --venv)/lib/python3.9/site-packages/
find . -type f | grep -v "__pycache__" | grep -v ".dist-info/" | sort | zip -9 -X -r --quiet $PREV/../tmp/wikipedia-libs.zip -@
cd $PREV

cp Dockerfile ../tmp/
docker build --no-cache -t wikipedia:latest ../tmp/

NOW=`date +%s`
REPOSITORY=`aws ecr describe-repositories | jq '.repositories[] | select(.repositoryUri | contains("wikipedia")) | .repositoryUri' -r`
echo "In repository $REPOSITORY"

aws ecr get-login-password | docker login --username AWS --password-stdin $REPOSITORY
docker tag wikipedia:latest $REPOSITORY:latest
docker push $REPOSITORY:latest

docker tag wikipedia:latest $REPOSITORY:$NOW
docker push $REPOSITORY:$NOW

aws lambda update-function-code --function-name wikipedia-run --image-uri $REPOSITORY:$NOW
aws lambda wait function-updated --function-name wikipedia-run