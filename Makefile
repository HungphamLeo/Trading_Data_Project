.PHONY: help init up down restart logs clean test build deploy

# Colors for output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
NC     := \033[0m # No Color

help: ## Show this help message
	@echo '$(GREEN)Available commands:$(NC)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

# ============================================
# Environment Setup
# ============================================

init: ## Initialize project (first time setup)
	@echo "$(GREEN)Initializing project...$(NC)"
	cp .env.example .env || true
	mkdir -p data/{bronze,silver,gold,samples,logs}
	mkdir -p output
	docker network create etl-network || true
	@echo "$(GREEN)✓ Project initialized$(NC)"

up: ## Start all services
	@echo "$(GREEN)Starting services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@$(MAKE) status

down: ## Stop all services
	@echo "$(YELLOW)Stopping services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart: ## Restart all services
	@$(MAKE) down
	@$(MAKE) up

status: ## Show service status
	@echo "$(GREEN)Service Status:$(NC)"
	@docker-compose ps

logs: ## Tail logs from all services
	docker-compose logs -f

logs-api: ## Tail logs from API Gateway
	docker-compose logs -f api-gateway

logs-processor: ## Tail logs from Data Processor
	docker-compose logs -f data-processor

logs-prefect: ## Tail logs from Prefect
	docker-compose logs -f prefect-server

# ============================================
# Development
# ============================================

build: ## Build all Docker images
	@echo "$(GREEN)Building images...$(NC)"
	docker-compose build
	@echo "$(GREEN)✓ Build complete$(NC)"

build-api: ## Build API Gateway image
	docker-compose build api-gateway

build-processor: ## Build Data Processor image
	docker-compose build data-processor

shell-api: ## Open shell in API Gateway container
	docker-compose exec api-gateway sh

shell-processor: ## Open shell in Data Processor container
	docker-compose exec data-processor bash

shell-db: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U dbt_user -d analytics_dw

# ============================================
# Data Operations
# ============================================

seed: ## Load sample data
	@echo "$(GREEN)Loading sample data...$(NC)"
	docker-compose exec data-processor python scripts/seed_data.py
	@echo "$(GREEN)✓ Sample data loaded$(NC)"

dbt-deps: ## Install dbt dependencies
	docker-compose exec dbt dbt deps

dbt-run: ## Run dbt models
	@echo "$(GREEN)Running dbt models...$(NC)"
	docker-compose exec dbt dbt run
	@echo "$(GREEN)✓ dbt run complete$(NC)"

dbt-test: ## Run dbt tests
	@echo "$(GREEN)Running dbt tests...$(NC)"
	docker-compose exec dbt dbt test
	@echo "$(GREEN)✓ dbt tests complete$(NC)"

dbt-snapshot: ## Run dbt snapshots
	@echo "$(GREEN)Running dbt snapshots...$(NC)"
	docker-compose exec dbt dbt snapshot
	@echo "$(GREEN)✓ dbt snapshots complete$(NC)"

dbt-docs: ## Generate and serve dbt docs
	docker-compose exec dbt dbt docs generate
	docker-compose exec dbt dbt docs serve --port 8081

pipeline-run: ## Run full ETL pipeline
	@echo "$(GREEN)Running full ETL pipeline...$(NC)"
	@$(MAKE) dbt-snapshot
	@$(MAKE) dbt-run
	@$(MAKE) dbt-test
	@echo "$(GREEN)✓ Pipeline complete$(NC)"

# ============================================
# Testing
# ============================================

test: ## Run all tests
	@$(MAKE) test-python
	@$(MAKE) test-go

test-python: ## Run Python unit tests
	@echo "$(GREEN)Running Python tests...$(NC)"
	docker-compose exec data-processor pytest tests/ -v

test-go: ## Run Go unit tests
	@echo "$(GREEN)Running Go tests...$(NC)"
	docker-compose exec api-gateway go test ./... -v

test-integration: ## Run integration tests
	@echo "$(GREEN)Running integration tests...$(NC)"
	docker-compose exec data-processor pytest tests/integration/ -v

lint-python: ## Lint Python code
	docker-compose exec data-processor flake8 app/
	docker-compose exec data-processor black --check app/

lint-go: ## Lint Go code
	docker-compose exec api-gateway golangci-lint run

format-python: ## Format Python code
	docker-compose exec data-processor black app/
	docker-compose exec data-processor isort app/

format-go: ## Format Go code
	docker-compose exec api-gateway gofmt -w .

# ============================================
# Monitoring
# ============================================

monitoring-up: ## Start monitoring stack (Prometheus, Grafana)
	docker-compose --profile monitoring up -d
	@echo "$(GREEN)✓ Monitoring stack started$(NC)"
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"

monitoring-down: ## Stop monitoring stack
	docker-compose --profile monitoring down

# ============================================
# Data Management
# ============================================

db-migrate: ## Run database migrations
	docker-compose exec data-processor alembic upgrade head

db-rollback: ## Rollback last database migration
	docker-compose exec data-processor alembic downgrade -1

db-backup: ## Backup PostgreSQL database
	@echo "$(GREEN)Backing up database...$(NC)"
	docker-compose exec -T postgres pg_dump -U dbt_user analytics_dw > backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup created$(NC)"

db-restore: ## Restore PostgreSQL database (usage: make db-restore FILE=backup.sql)
	@echo "$(YELLOW)Restoring database from $(FILE)...$(NC)"
	docker-compose exec -T postgres psql -U dbt_user analytics_dw < $(FILE)
	@echo "$(GREEN)✓ Database restored$(NC)"

minio-ui: ## Open MinIO UI
	@echo "$(GREEN)MinIO UI: http://localhost:9001$(NC)"
	@echo "Credentials: minioadmin / minioadmin123"

prefect-ui: ## Open Prefect UI
	@echo "$(GREEN)Prefect UI: http://localhost:4200$(NC)"

# ============================================
# Cleanup
# ============================================

clean: ## Remove all containers, volumes, and generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	docker-compose down -v
	rm -rf data/bronze/* data/silver/* data/gold/* data/logs/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

clean-data: ## Remove only data directories
	@echo "$(YELLOW)Cleaning data directories...$(NC)"
	rm -rf data/bronze/* data/silver/* data/gold/*
	@echo "$(GREEN)✓ Data cleaned$(NC)"

clean-logs: ## Remove log files
	rm -rf data/logs/*
	@echo "$(GREEN)✓ Logs cleaned$(NC)"

# ============================================
# CI/CD
# ============================================

ci-test: ## Run CI tests
	@$(MAKE) build
	@$(MAKE) test
	@$(MAKE) lint-python
	@$(MAKE) lint-go

deploy-dev: ## Deploy to development
	@echo "$(GREEN)Deploying to development...$(NC)"
	kubectl apply -f infra/kubernetes/base/
	kubectl apply -f infra/kubernetes/overlays/dev/

deploy-prod: ## Deploy to production
	@echo "$(YELLOW)Deploying to production...$(NC)"
	kubectl apply -f infra/kubernetes/overlays/prod/

# ============================================
# Utilities
# ============================================

fetch-rates: ## Fetch exchange rates (usage: make fetch-rates SYMBOL=BTCUSDT)
	@SYMBOL=${SYMBOL:-BTCUSDT}; \
	curl -X POST http://localhost:8080/api/v1/rates/fetch \
		-H "Content-Type: application/json" \
		-d "{\"symbol\":\"$$SYMBOL\",\"interval\":\"1h\",\"limit\":100}" | jq

ingest-csv: ## Ingest CSV file (usage: make ingest-csv FILE=data/samples/transactions.csv)
	docker-compose exec data-processor python -c "\
from app.flows.ingestion import ingest_csv_to_bronze; \
ingest_csv_to_bronze('$(FILE)', 'transactions', {})"

health-check: ## Check service health
	@echo "$(GREEN)Checking service health...$(NC)"
	@curl -s http://localhost:8080/health | jq || echo "API Gateway: DOWN"
	@curl -s http://localhost:4200/api/health | jq || echo "Prefect: DOWN"

ps: ## Show running containers
	@docker-compose ps

stats: ## Show container resource usage
	@docker stats --no-stream