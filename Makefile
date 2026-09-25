.PHONY: help start stop restart logs status setup setup-reverse demo check test-overlap clean

help:
	@echo "Redpanda Shadow Linking Demo - Available Commands"
	@echo ""
	@echo "  make start          - Start all services"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make logs           - View logs from all services"
	@echo "  make status         - Check service status"
	@echo "  make setup          - Initialize forward shadow link (source -> shadow)"
	@echo "  make setup-reverse  - Initialize reverse shadow link (shadow -> source), for bidirectional"
	@echo "  make demo           - Produce demo data"
	@echo "  make check          - Check replication status"
	@echo "  make test-overlap   - Run the overlapping-topic experiment (see docs/findings.md)"
	@echo "  make clean          - Stop and remove all containers and volumes"
	@echo ""
	@echo "Quick Start (one-directional):"
	@echo "  1. make start"
	@echo "  2. make setup"
	@echo "  3. make demo"
	@echo "  4. make check"
	@echo ""
	@echo "Bidirectional:"
	@echo "  1. make start"
	@echo "  2. make setup && make setup-reverse"
	@echo "  3. make test-overlap   (to see what happens with overlapping topics)"

start:
	@echo "Starting Redpanda shadow demo environment..."
	docker compose up -d
	@echo ""
	@echo "Waiting for services to be ready..."
	@sleep 5
	@docker compose ps
	@echo ""
	@echo "Services started!"
	@echo "  Source Console: http://localhost:8080"
	@echo "  Shadow Console: http://localhost:8081"

stop:
	@echo "Stopping services..."
	docker compose stop

restart:
	@echo "Restarting services..."
	docker compose restart

logs:
	docker compose logs -f

status:
	@docker compose ps

setup:
	@echo "Initializing shadow link..."
	docker exec rpk-client /scripts/setup-shadow-link.sh

setup-reverse:
	@echo "Initializing reverse shadow link for bidirectional replication..."
	docker exec rpk-client /scripts/setup-reverse-shadow-link.sh

test-overlap:
	@echo "Running overlapping-topic experiment..."
	docker exec rpk-client /scripts/test-overlap.sh

demo:
	@echo "Producing demo data..."
	docker exec rpk-client /scripts/demo-produce.sh

check:
	@echo "Checking replication status..."
	docker exec rpk-client /scripts/check-replication.sh

clean:
	@echo "Stopping and removing all containers and volumes..."
	docker compose down -v
	@echo "Cleanup complete!"
