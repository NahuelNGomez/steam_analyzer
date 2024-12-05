#!/bin/bash

INTERVAL=20

while true; do
    containers=$(docker ps --format '{{.ID}} {{.Names}}' | grep -vE '(rabbitmq|gateway|client1)' | awk '{print $1}')

    containers_array=($containers)

    if [ ${#containers_array[@]} -gt 0 ]; then
        random_index=$((RANDOM % ${#containers_array[@]}))
        container_to_stop=${containers_array[$random_index]}

        echo "Deteniendo contenedor: $container_to_stop"
        docker stop "$container_to_stop"
    else
        echo "No hay contenedores disponibles para detener."
    fi

    sleep $INTERVAL
done
