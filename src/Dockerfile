FROM python:3.9.1-slim-buster AS builder
COPY wikipedia-*.zip /app/wikipedia/

RUN apt-get update && apt-get install -y zip && apt-get clean \
    && ls -l /app/wikipedia/ && cd /app/wikipedia/ \
    && unzip wikipedia-app.zip && unzip wikipedia-libs.zip \
    && rm wikipedia-*.zip

FROM python:3.9.1-slim-buster
COPY --from=builder /app/wikipedia/ /app/wikipedia/

CMD [ "python", "-u", "/app/wikipedia/index.py" ]