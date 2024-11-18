FROM python:3.9.7-alpine
RUN apk update
RUN apk add docker

COPY /doctor /src
COPY /common /src/common
WORKDIR /src

ENTRYPOINT ["python3", "main.py"]
