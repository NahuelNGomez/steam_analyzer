#!/bin/bash

# Script para reiniciar el servicio de RabbitMQ
echo "Reiniciando RabbitMQ..."
docker compose -f docker-compose-rabbit.yaml down
docker compose -f docker-compose-rabbit.yaml up -d --build

if [ $? -eq 0 ]; then
  echo "RabbitMQ se ha reiniciado correctamente."\else
  echo "Hubo un error al reiniciar RabbitMQ."
fi