.PHONY: build test lint fmt clean help install dev benchmark

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install package with dev dependencies
	pip install -e ".[dev]"

build: ## Build the package
	python -m build

test: ## Run tests
	pytest tests/

lint: ## Run linting
	ruff check .
	mypy

fmt: ## Format code
	ruff format .

clean: ## Clean build artifacts
	rm -rf dist/ build/ *.egg-info .pytest_cache .mypy_cache

benchmark: ## Run benchmarks
	pytest benchmarks/ --benchmark-group-by=group --benchmark-sort=fullname

dev: install ## Set up development environment
	pre-commit install 2>/dev/null || true
