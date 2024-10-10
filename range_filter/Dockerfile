FROM python:3.9-slim

# Copiar los archivos necesarios
COPY . /

# Instalar dependencias
RUN pip install --no-cache-dir pika

# Comando para ejecutar el script principal
CMD ["python3", "main.py"]
