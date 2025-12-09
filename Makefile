.PHONY: help build up down restart logs clean ingest load dbt-deps dbt-snapshot dbt-run dbt-test dbt-docs full-pipeline status

COMPOSE_FILE := infra/docker_compose/core.yml

help:
	@echo "Crypto Data Platform - Available Commands"
	@echo ""
	@echo "Infrastructure Management:"
	@echo "  make build          - Build Docker images"
	@echo "  make up             - Start all services"
	@echo "  make down           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make status         - Check services status"
	@echo "  make logs           - View logs from all services"
	@echo "  make logs-ingest    - View ingestion logs"
	@echo "  make logs-dbt       - View DBT logs"
	@echo "  make logs-postgres  - View PostgreSQL logs"
	@echo "  make clean          - Stop services and clean data"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make ingest         - Fetch data from Binance API"
	@echo "  make load           - Load data to PostgreSQL"
	@echo "  make dbt-deps       - Install DBT dependencies"
	@echo "  make dbt-snapshot   - Run DBT snapshots"
	@echo "  make dbt-run        - Run all DBT models"
	@echo "  make dbt-test       - Run DBT tests"
	@echo "  make dbt-docs       - Generate and serve DBT docs"
	@echo "  make full-pipeline  - Run complete pipeline end-to-end"
	@echo ""
	@echo "Data Verification:"
	@echo "  make verify-db      - Verify database tables"
	@echo "  make psql           - Connect to PostgreSQL"
	@echo "  make query-sample   - Run sample queries"
	@echo ""
	@echo "Development:"
	@echo "  make shell-ingest   - Shell into ingestion container"
	@echo "  make shell-dbt      - Shell into DBT container"
	@echo "  make shell-postgres - Shell into PostgreSQL"

# ==========================================
# Infrastructure Commands
# ==========================================

build:
	@echo "Building Docker images..."
	docker-compose -f $(COMPOSE_FILE) build

up:
	@echo "Starting services..."
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	@docker-compose -f $(COMPOSE_FILE) exec -T postgres pg_isready -U dataeng || true
	@echo "All services started"
	@make status

down:
	@echo "Stopping services..."
	docker-compose -f $(COMPOSE_FILE) down

restart: down up

status:
	@echo "Services Status:"
	@docker-compose -f $(COMPOSE_FILE) ps

logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

logs-ingest:
	docker-compose -f $(COMPOSE_FILE) logs -f ingestion

logs-dbt:
	docker-compose -f $(COMPOSE_FILE) logs -f dbt

logs-postgres:
	docker-compose -f $(COMPOSE_FILE) logs -f postgres

clean:
	@echo "Cleaning up..."
	docker-compose -f $(COMPOSE_FILE) down -v
	@echo "Removing output files..."
	rm -rf output/raw_rates/*.parquet
	rm -rf logger_storage/*.log
	@echo "Cleanup complete"

# ==========================================
# Data Pipeline Commands
# ==========================================

ingest:
	@echo "Starting Binance data ingestion..."
	docker-compose -f $(COMPOSE_FILE) exec ingestion python scripts/run_binance_ingestion.py
	@echo "Ingestion complete"

load:
	@echo "Loading data to PostgreSQL..."
	docker-compose -f $(COMPOSE_FILE) exec ingestion python scripts/load_data_to_postgres.py
	@echo "Data loaded"

dbt-deps:
	@echo "Installing DBT dependencies..."
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt deps
	@echo "Dependencies installed"

dbt-snapshot:
	@echo "Running DBT snapshots..."
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt snapshot
	@echo "Snapshots complete"

dbt-run:
	@echo "Running DBT models..."
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt run
	@echo "DBT models built"

dbt-test:
	@echo "Running DBT tests..."
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt test
	@echo "Tests complete"

dbt-docs:
	@echo "Generating DBT documentation..."
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt docs generate
	@echo "Serving docs at http://localhost:8080"
	docker-compose -f $(COMPOSE_FILE) exec dbt dbt docs serve --port 8080

full-pipeline:
	@echo ""
	@echo "Running Full Data Pipeline"
	@echo ""
	@echo "Step 1/5: Ingesting data from Binance API..."
	@make ingest
	@echo ""
	@echo "Step 2/5: Loading data to PostgreSQL..."
	@make load
	@echo ""
	@echo "Step 3/5: Running DBT snapshots..."
	@make dbt-snapshot
	@echo ""
	@echo "Step 4/5: Running DBT transformations..."
	@make dbt-run
	@echo ""
	@echo "Step 5/5: Running data quality tests..."
	@make dbt-test
	@echo ""
	@echo "Pipeline Complete"
	@echo ""
	@make verify-db

# ==========================================
# Data Verification Commands
# ==========================================

verify-db:
	@echo "Verifying database..."
	@docker-compose -f $(COMPOSE_FILE) exec -T postgres psql -U dataeng -d dwh -c "\
		SELECT 'transactions' as table_name, COUNT(*) as row_count FROM transactions \
		UNION ALL \
		SELECT 'users', COUNT(*) FROM users \
		UNION ALL \
		SELECT 'rates', COUNT(*) FROM rates \
		UNION ALL \
		SELECT 'fact_transactions', COUNT(*) FROM marts.marts_core__fact_transactions;"

psql:
	@echo "Connecting to PostgreSQL..."
	docker-compose -f $(COMPOSE_FILE) exec postgres psql -U dataeng -d dwh

query-sample:
	@echo "Running sample queries..."
	@docker-compose -f $(COMPOSE_FILE) exec -T postgres psql -U dataeng -d dwh -f /scripts/sample_bi_queries.sql || true

# ==========================================
# Development Commands
# ==========================================

shell-ingest:
	@echo "Opening shell in ingestion container..."
	docker-compose -f $(COMPOSE_FILE) exec ingestion bash

shell-dbt:
	@echo "Opening shell in DBT container..."
	docker-compose -f $(COMPOSE_FILE) exec dbt bash

shell-postgres:
	@echo "Opening shell in PostgreSQL container..."
	docker-compose -f $(COMPOSE_FILE) exec postgres bash

# ==========================================
# Quick Commands
# ==========================================

quick-start: build up full-pipeline
	@echo "Quick start complete"

test-connection:
	@echo "Testing database connection..."
	@docker-compose -f $(COMPOSE_FILE) exec -T postgres pg_isready -U dataeng && echo "PostgreSQL is ready" || echo "PostgreSQL is not ready"
	@docker-compose -f $(COMPOSE_FILE) exec -T redis redis-cli ping | grep -q PONG && echo "Redis is ready" || echo "Redis is not ready"