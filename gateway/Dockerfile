# gateway/Dockerfile

FROM python:3.9.7-slim

WORKDIR /gateway

COPY . /gateway
COPY ./common /common

# Añadir common al PYTHONPATH
ENV PYTHONPATH="/common"

RUN pip install --no-cache-dir pika

CMD ["python3", "main.py"]
