# gateway/Dockerfile

FROM python:3.9.7-slim

COPY /gateway /src
COPY /common /src/common
WORKDIR /src

RUN pip install --no-cache-dir pika


CMD ["python3", "main.py"]
