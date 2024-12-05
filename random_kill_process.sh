#!/bin/bash

INTERVAL=30

# Contenedores que deben excluirse
EXCLUDE_CONTAINERS="(rabbitmq|gateway|client1|client2|negative_review_filter|positive_review_filter|positive_review_filter_2|positive_review_filter3|positive_review_filter4)"

while true; do
    # Obtener los contenedores en ejecución, excluyendo los especificados
    containers=$(docker ps --format '{{.Names}}' | grep -vE "$EXCLUDE_CONTAINERS")

    # Convertir la lista de contenedores en un array
    containers_array=($containers)

    if [ ${#containers_array[@]} -ge 2 ]; then
        # Seleccionar 2 índices aleatorios distintos
        selected_indices=($(shuf -i 0-$((${#containers_array[@]} - 1)) -n 2))
        
        # Obtener los nombres de los contenedores seleccionados
        containers_to_stop=("${containers_array[${selected_indices[0]}]}" "${containers_array[${selected_indices[1]}]}")

        # Detener los contenedores seleccionados
        echo "Deteniendo contenedores: ${containers_to_stop[*]}"
        for container in "${containers_to_stop[@]}"; do
            docker stop "$container"
        done
    else
        echo "No hay suficientes contenedores disponibles para detener."
    fi

    sleep $INTERVAL
done
