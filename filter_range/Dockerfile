# filter_range/Dockerfile

FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos necesarios
COPY . /app

# Instalar dependencias
RUN pip install --no-cache-dir pika

# Comando para ejecutar el script principal
CMD ["python3", "main.py"]
