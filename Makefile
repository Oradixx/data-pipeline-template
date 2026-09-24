.DEFAULT_GOAL := help
# Load .env (if present) so DATABASE_URL etc. are available to local commands.
-include .env
export

.PHONY: help install env up down reset logs ps psql migrate ingest lint format typecheck test test-integration check clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-17s\033[0m %s\n", $$1, $$2}'

# ---------- setup ----------
install: env ## Create the venv, install deps and git hooks
	uv sync
	uv run pre-commit install

env: ## Create .env from .env.example (if missing)
	@test -f .env || (cp .env.example .env && echo "created .env — change the passwords")

# ---------- stack ----------
up: env ## Build and start the whole stack in the background
	docker compose up -d --build
	@echo "Grafana: http://localhost:$${GRAFANA_PORT:-3000}"

down: ## Stop the stack (data is kept)
	docker compose down

reset: ## Stop the stack AND delete its data volumes
	docker compose down --volumes

logs: ## Follow the ingestion logs
	docker compose logs -f ingest

ps: ## Show the services' status
	docker compose ps

psql: ## Open a SQL shell in the database
	docker compose exec db psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

# ---------- run locally (outside Docker, DB from `make up`) ----------
migrate: ## Apply migrations from your machine
	uv run pipeline migrate

ingest: ## Run the ingestion from your machine (Ctrl+C to stop)
	uv run pipeline ingest

# ---------- quality ----------
lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## Static type checking
	uv run mypy

test: ## Unit tests (no database needed)
	uv run pytest tests/unit

test-integration: ## Integration tests (needs `make up`)
	uv run pytest tests/integration --no-cov

check: lint typecheck test ## Lint, types and unit tests

clean: ## Remove caches
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
