#!/bin/bash

# Verifica que el script esté siendo ejecutado desde la raíz del proyecto
if [ ! -f "Makefile" ]; then
  echo "Error: Este script debe ejecutarse desde la raíz del proyecto."
  exit 1
fi

# Encuentra y elimina todos los subdirectorios llamados 'persistence'
echo "Buscando y eliminando subdirectorios llamados 'persistence'..."
find . -type d -name "persistence" -exec rm -rf {} +

# Mensaje de confirmación
echo "Todos los subdirectorios 'persistence' han sido eliminados."
