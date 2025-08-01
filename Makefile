.PHONY: help build migration backfill realtime transform status clean logs

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Essential Commands:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $1, $2}' $(MAKEFILE_LIST)

build: ## Build Docker images
	docker compose build

# The 4 Essential Operations
migration: ## Set up database schema and tables
	docker compose --profile migration up

backfill: ## Load historical raw data (uses START_SLOT/END_SLOT from .env)
	docker compose --profile backfill up -d

realtime: ## Load new raw data continuously
	docker compose --profile realtime up -d

transform: ## Process raw data into structured tables
	docker compose --profile transform up -d

transform-continuous: ## Process raw data into structured tables
	docker compose --profile transform-continuous up -d

# Monitoring
status: ## Show processing progress overview
	docker run --rm --env-file .env beacon-indexer:latest python scripts/chunks.py overview

logs: ## Show logs for running services
	docker compose logs -f

clean: ## Stop all services and clean up
	docker compose down -v
	docker system prune -f