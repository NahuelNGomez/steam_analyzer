SHELL := /bin/bash
PROJECT_NAME = tp1

default: docker-compose-up

docker-compose-up:
	docker compose -f docker-compose-system.yaml up -d --build
.PHONY: docker-compose-up

docker-rabbit-up:
	# Ejecuta RabbitMQ
	docker compose -f docker-compose-rabbit.yaml up -d --build --remove-orphans
.PHONY: docker-rabbit-up

docker-system-up:
	# Levanta primero RabbitMQ y luego el sistema
	make docker-rabbit-up
	docker compose -f docker-compose-system.yaml up -d --build
.PHONY: docker-system-up

docker-system-down:
	# Baja el sistema y RabbitMQ
	make docker rabbit-down
	make docker-compose-down
.PHONY: docker-system-down

docker-compose-down:
	# Baja solo el sistema sin incluir RabbitMQ
	docker compose -f docker-compose-system.yaml down
.PHONY: docker-compose-down

docker-rabbit-down:
	# Baja solo RabbitMQ
	docker compose -f docker-compose-rabbit.yaml down
.PHONY: docker-rabbit-down

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