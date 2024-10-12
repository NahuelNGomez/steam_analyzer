#!/bin/bash

# Hacer "make docker-compose-down"
echo "Deteniendo los servicios actuales..."
make docker-rabbit-down
make docker-compose-down


# Hacer "make docker-system-up"
echo "Levantando servicios..."
make docker-rabbit-up
make docker-compose-up

echo "Sistema reiniciado exitosamente."