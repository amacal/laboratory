FROM python:3.9.0-buster

COPY wikipedia-*.zip /app/wikipedia/
RUN ls -l /app/wikipedia/ && cd /app/wikipedia/ \
    && unzip wikipedia-app.zip && unzip wikipedia-libs.zip \
    && rm wikipedia-*.zip

CMD [ "python", "-u", "/app/wikipedia/index.py" ]