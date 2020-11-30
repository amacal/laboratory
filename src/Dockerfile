FROM python:3.9.0-buster

COPY wikipedia-*.zip /app/wikipedia/
RUN apt-get update && apt-get install jq -y && ls -l /app/wikipedia/ && cd /app/wikipedia/ \
    && unzip wikipedia-app.zip && unzip wikipedia-libs.zip \
    && rm wikipedia-*.zip && curl -O https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py && pip install --upgrade awscli

#CMD [ "bash", "/app/wikipedia/start.sh" ]
CMD [ "python", "-u", "/app/wikipedia/index.py" ]