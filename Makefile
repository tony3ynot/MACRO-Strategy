SHELL := /bin/bash
.DEFAULT_GOAL := help

# Auto-create .env from example if missing
.env:
	@cp .env.example .env
	@echo "Created .env from .env.example. Review before pushing or deploying."

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: .env ## Start all services (postgres, redis, app, worker)
	docker compose up -d

down: ## Stop all services
	docker compose down

build: ## (Re)build images
	docker compose build

rebuild: ## Force-rebuild without cache
	docker compose build --no-cache

logs: ## Tail logs (all services)
	docker compose logs -f --tail=100

logs-app: ## Tail app logs only
	docker compose logs -f --tail=100 app

logs-worker: ## Tail worker logs only
	docker compose logs -f --tail=100 worker

ps: ## Show service status
	docker compose ps

restart: ## Restart all services
	docker compose restart

shell-app: ## Bash into app container
	docker compose exec app bash

shell-pg: ## psql into postgres
	docker compose exec postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro}

shell-redis: ## redis-cli into redis
	docker compose exec redis redis-cli

verify-health: ## Curl /health endpoint
	@curl -fsS http://localhost:$${API_PORT:-8000}/health | python3 -m json.tool

verify-tsdb: ## Confirm TimescaleDB extension is loaded
	@docker compose exec -T postgres psql -U $${PG_USER:-macro} -d $${PG_DB:-macro} \
		-c "SELECT extname, extversion FROM pg_extension WHERE extname='timescaledb';"

jupyter-up: .env ## Start Jupyter alongside core stack (research env)
	docker compose -f docker-compose.yml -f docker-compose.jupyter.yml up -d

jupyter-down: ## Stop Jupyter only
	docker compose -f docker-compose.yml -f docker-compose.jupyter.yml stop jupyter

clean: ## Stop and REMOVE volumes (DESTROYS DATA!)
	@read -p "This will delete all DB data. Continue? [y/N] " ans && \
		[ "$$ans" = "y" ] && docker compose down -v || echo "Aborted."

.PHONY: help up down build rebuild logs logs-app logs-worker ps restart \
        shell-app shell-pg shell-redis verify-health verify-tsdb \
        jupyter-up jupyter-down clean
