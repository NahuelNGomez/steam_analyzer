#!/bin/bash

# Verificar que se recibieron dos parámetros
if [ "$#" -ne 2 ]; then
  echo "Uso: $0 <archivo1> <archivo2>"
  exit 1
fi

# Archivos a comparar
FILE1="$1"
FILE2="$2"

# Verificar que ambos archivos existen
if [ ! -f "$FILE1" ]; then
  echo "Error: El archivo '$FILE1' no existe."
  exit 1
fi

if [ ! -f "$FILE2" ]; then
  echo "Error: El archivo '$FILE2' no existe."
  exit 1
fi

# Comparar los archivos usando diff
DIFF_OUTPUT=$(diff -u "$FILE1" "$FILE2")

# Mostrar el resultado de la comparación
if [ -z "$DIFF_OUTPUT" ]; then
  echo "No se encontraron diferencias, los resultados son equivalentes."
else
  echo -e "\nDiferencias encontradas:"
  echo "$DIFF_OUTPUT"
fi

#USO ./compare_results.sh results/responses.json results/serial_queries_1_results.json