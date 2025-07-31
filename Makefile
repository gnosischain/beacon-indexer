.PHONY: help install build clean migration backfill realtime transform logs \
         status chunks-detail test-connection \
         dev-migration dev-backfill dev-realtime dev-transform \
         restart-backfill restart-realtime restart-transform shell

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $1, $2}' $(MAKEFILE_LIST)

install: ## Install Python dependencies
	pip install -r requirements.txt

build: ## Build Docker images
	docker compose build

# The 4 Essential Modes
migration: ## Set up database schema
	docker compose --profile migration up

backfill: ## Load historical raw data
	docker compose --profile backfill up

realtime: ## Load new raw data continuously  
	docker compose --profile realtime up -d

transform: ## Process raw data into structured tables
	docker compose --profile transform up -d

# Monitoring (automatic - no manual fixes needed)
status: ## Show chunk progress
	docker run --rm --env-file .env beacon-indexer:latest python scripts/chunks.py overview

chunks-detail: ## Show detailed chunk status
	docker run --rm --env-file .env beacon-indexer:latest python scripts/chunks.py status

test-connection: ## Test ClickHouse connection
	docker run --rm --env-file .env beacon-indexer:latest python scripts/chunks.py test

# Utility targets
logs: ## Show logs for running services
	docker compose logs -f

clean: ## Clean up Docker resources
	docker compose down -v
	docker system prune -f

# Docker-based development targets
dev-migration: ## Run migration in Docker
	docker run --rm --env-file .env beacon-indexer:latest python scripts/migrate.py

dev-backfill: ## Run backfill in Docker (uses START_SLOT and END_SLOT from .env)
	docker run --rm --env-file .env beacon-indexer:latest python -m src.main load backfill

dev-realtime: ## Run realtime loader in Docker
	docker run --rm --env-file .env beacon-indexer:latest python -m src.main load realtime

dev-transform: ## Run transformer in Docker
	docker run --rm --env-file .env beacon-indexer:latest python -m src.main transform run

# Service management
restart-backfill: ## Stop and restart backfill service
	docker compose --profile backfill down
	docker compose --profile backfill up

restart-realtime: ## Stop and restart realtime service
	docker compose --profile realtime down
	docker compose --profile realtime up -d

restart-transform: ## Stop and restart transform service
	docker compose --profile transform down
	docker compose --profile transform up -d

# Development helpers
shell: ## Open interactive shell in Docker container
	docker run --rm -it --env-file .env beacon-indexer:latest /bin/bash

tail-logs: ## Show recent logs
	docker compose logs --tail=50

follow-logs: logs ## Alias for logs