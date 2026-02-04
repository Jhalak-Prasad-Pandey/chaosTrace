.PHONY: help install dev test lint format typecheck clean docker-up docker-down run-api

# Default target
help:
	@echo "ChaosTrace Development Commands"
	@echo "================================"
	@echo "install     - Install production dependencies"
	@echo "dev         - Install development dependencies"
	@echo "test        - Run test suite"
	@echo "lint        - Run linter"
	@echo "format      - Format code"
	@echo "typecheck   - Run type checker"
	@echo "clean       - Remove build artifacts"
	@echo "docker-up   - Start development containers"
	@echo "docker-down - Stop development containers"
	@echo "run-api     - Run the API server"

# Installation
install:
	pip install -e .

dev:
	pip install -e ".[dev]"
	pre-commit install

# Testing
test:
	pytest tests/ -v --cov=chaostrace --cov-report=term-missing

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

# Code Quality
lint:
	ruff check chaostrace/ tests/

format:
	ruff format chaostrace/ tests/
	ruff check --fix chaostrace/ tests/

typecheck:
	mypy chaostrace/

# Docker
docker-up:
	docker compose -f sandbox/docker-compose.yaml up -d

docker-down:
	docker compose -f sandbox/docker-compose.yaml down -v

docker-build:
	docker compose -f sandbox/docker-compose.yaml build

# Run
run-api:
	uvicorn chaostrace.control_plane.main:app --reload --host 0.0.0.0 --port 8000

run-proxy:
	python -m chaostrace.db_proxy.proxy_server

# Cleanup
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
