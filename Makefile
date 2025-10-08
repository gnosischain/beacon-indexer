.PHONY: help build migration backfill realtime transform status clean logs
.PHONY: maintenance-status maintenance-check maintenance-fix maintenance-reset maintenance-failed maintenance-gaps maintenance-transform

# Default maintenance parameters
MAINTENANCE_START_SLOT ?= 0
MAINTENANCE_END_SLOT ?= 999999999
MAINTENANCE_RECENT_START ?= 9000000
MAINTENANCE_RECENT_END ?= 9100000

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Essential Commands:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build Docker images
	docker build -t beacon-indexer:latest . --no-cache

# The 4 Essential Operations
migration: ## Set up database schema and tables
	docker compose --profile migration up

backfill: ## Load historical raw data (uses START_SLOT/END_SLOT from .env)
	docker compose --profile backfill up -d

realtime: ## Load new raw data continuously
	docker compose --profile realtime up -d

transform: ## Process raw data into structured tables (batch mode)
	docker compose --profile transform up

transform-continuous: ## Process raw data continuously
	docker compose --profile transform-continuous up -d

# Monitoring
status: ## Show processing progress overview
	docker run --rm --env-file .env beacon-indexer:latest python scripts/chunks.py overview

logs: ## Show logs for running services
	docker compose logs -f

clean: ## Stop all services and clean up
	docker compose down -v
	docker system prune -f

# Maintenance Commands

maintenance-status: ## Show system status and recommendations
	@echo "🔍 System Status..."
	docker compose run --rm maintenance python scripts/maintenance.py summary
	docker compose run --rm maintenance python scripts/maintenance.py recommendations

maintenance-transform: ## Process untransformed chunks
	@echo "⚡ Processing untransformed chunks..."
	docker compose run --rm maintenance python -m src.main transform batch

maintenance-failed: ## Show all failed chunks
	@echo "❌ Failed chunks..."
	docker compose run --rm maintenance python scripts/maintenance.py failed

maintenance-gaps: ## Analyze data gaps
	@echo "🕳️ Data gaps analysis..."
	docker compose run --rm maintenance python scripts/maintenance.py gaps

maintenance-simple-check: ## Simple status check (failed chunks & untransformed)
	@echo "🔍 Simple status check..."
	docker compose run --rm maintenance python -m src.main maintain check --start-slot $(MAINTENANCE_RECENT_START) --end-slot $(MAINTENANCE_RECENT_END)

maintenance-fix: ## Fix failed chunks in recent range (9M-9.1M slots)
	@echo "🔧 Fixing failed chunks in recent range..."
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_RECENT_START) --end-slot $(MAINTENANCE_RECENT_END)

maintenance-fix-force: ## Force reprocess recent range (9M-9.1M slots)
	@echo "🔧 Force reprocessing recent range..."
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_RECENT_START) --end-slot $(MAINTENANCE_RECENT_END) --force

maintenance-fix-force-loaders: ## Force reprocess recent range for specific loaders (set LOADERS="blocks,validators")
	@echo "🔧 Force reprocessing recent range for loaders: $(LOADERS)"
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_RECENT_START) --end-slot $(MAINTENANCE_RECENT_END) --force --loaders $(LOADERS)

maintenance-fix-all: ## Fix all failed chunks system-wide
	@echo "🔧 Fixing all failed chunks..."
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT)

maintenance-reset: ## Reset all failed chunks to pending
	@echo "🔄 Resetting failed chunks to pending..."
	docker compose run --rm maintenance python -m src.main maintain reset --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT) --status failed

maintenance-reset-stuck: ## Reset stuck (claimed) chunks to pending
	@echo "🔄 Resetting stuck chunks to pending..."
	docker compose run --rm maintenance python -m src.main maintain reset --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT) --status claimed

maintenance-dry-run: ## Preview what would be fixed in recent range
	@echo "🧪 Dry run for recent range..."
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_RECENT_START) --end-slot $(MAINTENANCE_RECENT_END) --dry-run

# Custom range commands (override with environment variables)
maintenance-check-custom: ## Check custom range (set MAINTENANCE_START_SLOT/END_SLOT)
	@echo "🔍 Custom range check: $(MAINTENANCE_START_SLOT) to $(MAINTENANCE_END_SLOT)"
	docker compose run --rm maintenance python -m src.main maintain check --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT)

maintenance-fix-custom: ## Fix custom range (set MAINTENANCE_START_SLOT/END_SLOT)
	@echo "🔧 Custom range fix: $(MAINTENANCE_START_SLOT) to $(MAINTENANCE_END_SLOT)"
	docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT)

maintenance-fix-custom-force: ## Force fix custom range with loaders (set MAINTENANCE_START_SLOT/END_SLOT and LOADERS)
	@echo "🔧 Custom range force fix: $(MAINTENANCE_START_SLOT) to $(MAINTENANCE_END_SLOT)"
	@if [ -n "$(LOADERS)" ]; then \
		echo "   Loaders: $(LOADERS)"; \
		docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT) --force --loaders $(LOADERS); \
	else \
		docker compose run --rm maintenance python -m src.main maintain fix --start-slot $(MAINTENANCE_START_SLOT) --end-slot $(MAINTENANCE_END_SLOT) --force; \
	fi

maintenance-help: ## Show maintenance help
	@echo "🛠️  Simple Maintenance Commands"
	@echo ""
	@echo "DAILY COMMANDS:"
	@echo "  make maintenance-status          - System status & recommendations"
	@echo "  make maintenance-transform       - Process untransformed chunks"
	@echo "  make maintenance-failed          - Show failed chunks"
	@echo ""
	@echo "FIX COMMANDS:"
	@echo "  make maintenance-fix             - Fix failed chunks (recent range)"
	@echo "  make maintenance-fix-force       - Force reprocess recent range (all loaders)"
	@echo "  make maintenance-fix-all         - Fix all failed chunks system-wide"
	@echo "  make maintenance-reset           - Reset failed chunks to pending"
	@echo "  make maintenance-reset-stuck     - Reset stuck workers"
	@echo ""
	@echo "CHECK COMMANDS:"
	@echo "  make maintenance-simple-check    - Simple status check (failed & untransformed)"
	@echo "  make maintenance-gaps            - Data gaps analysis"
	@echo ""
	@echo "EXAMPLES WITH LOADERS:"
	@echo "  LOADERS='blocks' make maintenance-fix-force-loaders      # Force fix blocks only"
	@echo "  LOADERS='blocks,validators' make maintenance-fix-force-loaders"
	@echo ""
	@echo "CUSTOM RANGES:"
	@echo "  MAINTENANCE_START_SLOT=8000000 MAINTENANCE_END_SLOT=8100000 make maintenance-fix-custom"
	@echo "  MAINTENANCE_START_SLOT=8000000 MAINTENANCE_END_SLOT=8100000 LOADERS='blocks' make maintenance-fix-custom-force"
	@echo ""
	@echo "QUICK SOLUTIONS:"
	@echo "  make maintenance-transform       # Fix untransformed data"
	@echo "  make maintenance-fix            # Fix recent failures"
	@echo "  make maintenance-reset-stuck    # Fix stuck workers"