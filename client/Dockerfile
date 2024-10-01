# client/Dockerfile

# Utiliza una imagen base de Python
FROM python:3.9.7-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /client

# Copia todos los archivos del contexto al directorio /client en el contenedor
COPY . /client

# Instala las dependencias necesarias
RUN pip install --no-cache-dir pika

# Comando para ejecutar el cliente
CMD ["python3", "client.py"]
