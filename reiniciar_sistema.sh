#!/bin/bash

# Hacer "make docker-compose-down"
echo "Deteniendo los servicios actuales..."
make docker-rabbit-down
make docker-compose-down


# Hacer "make docker-system-up"
echo "Levantando servicios..."
make docker-rabbit-up

# Esperar 5 segundos antes de que inicie rabbit
for i in {5..1}; do
  echo "Esperando $i segundos..."
  sleep 1
done


make docker-compose-up
echo "Sistema reiniciado exitosamente."