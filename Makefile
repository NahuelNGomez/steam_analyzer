# Makefile

SHELL := /bin/bash
PROJECT_NAME = tp1

default: docker-compose-up

docker-compose-up:
	# Ejecuta RabbitMQ primero
	docker compose -f docker-compose-system.yaml up -d --build

.PHONY: docker-compose-up


docker-rabbit-up:
	# Ejecuta RabbitMQ primero
	docker compose -f docker-compose-rabbit.yaml up -d --build
.PHONY: docker-rabbit-up

docker-compose-down:
	# Baja ambos archivos docker-compose en el orden inverso
	docker compose -f docker-compose-system.yaml -f docker-compose-rabbit.yaml down

.PHONY: docker-compose-down

docker-compose-logs:
	# Mostrar logs de ambos archivos
	docker compose -f docker-compose-rabbit.yaml -f docker-compose-system.yaml logs -f

.PHONY: docker-compose-logs

docker-compose-rebuild:
	# Reconstruir ambos sistemas sin caché
	docker compose -f docker-compose-rabbit.yaml -f docker-compose-system.yaml build --no-cache

.PHONY: docker-compose-rebuild

clean:
	# Limpia imágenes no utilizadas y contenedores detenidos
	docker system prune -f

.PHONY: clean
