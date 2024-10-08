FROM python:3.9-slim
COPY . /

# Instalar dependencias
RUN pip install --no-cache-dir pika langid

# Comando para ejecutar el script principal
CMD ["python3", "main.py"]
