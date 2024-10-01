SHELL := /bin/bash
PROJECT_NAME = tp1

default: docker-compose-up

docker-compose-up:
	# Ejecuta este comando de vez en cuando para limpiar etapas intermedias generadas 
	# durante la construcción de las imágenes (tu disco duro lo agradecerá :) ). 
	# No lo dejes descomentado si deseas evitar reconstruir imágenes innecesariamente.
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
	docker compose -f docker-compose-dev.yaml up -d --build

.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml down

.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f

.PHONY: docker-compose-logs

docker-compose-rebuild:
	docker compose -f docker-compose-dev.yaml build --no-cache

.PHONY: docker-compose-rebuild

clean:
	# Limpia imágenes no utilizadas y contenedores detenidos
	docker system prune -f

.PHONY: clean
