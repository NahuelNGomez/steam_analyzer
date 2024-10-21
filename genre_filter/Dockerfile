FROM python:3.9-slim

COPY /genre_filter /src
COPY /common /src/common
WORKDIR /src

# Instalar dependencias
RUN pip install --no-cache-dir pika

# Comando para ejecutar el script principal
CMD ["python3", "main.py"]
