#!/bin/bash

# Hacer "make docker-compose-down"
echo "Deteniendo los servicios actuales..."
make docker-system-down


# Hacer "make docker-system-up"
echo "Levantando servicios..."
make docker-system-up

echo "Sistema reiniciado exitosamente."