FROM python:3.9.7-slim
RUN pip install pika
COPY client /
COPY /data /data

ENTRYPOINT ["/bin/sh"]