# gateway/Dockerfile
FROM python:3.9.7-slim

# Instalar dependencias necesarias
RUN pip install --no-cache-dir pika

# Establecer el directorio de trabajo
WORKDIR /gateway

# Copiar todos los archivos del contexto al directorio /gateway en el contenedor
COPY . /gateway

# Definir el comando para ejecutar el script Python
CMD ["python3", "main.py"]
