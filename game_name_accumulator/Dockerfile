FROM python:3.9.7-slim
RUN pip install pika
COPY /game_name_accumulator /src
COPY /common /src/common
WORKDIR /src
ENTRYPOINT ["python3", "main.py"]