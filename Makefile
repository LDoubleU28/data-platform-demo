.PHONY: up down test run validate

up:
	docker compose up -d

down:
	docker compose down -v

test:
	.venv/bin/python -m pytest -q

run:
	.venv/bin/python scripts/run_pipeline.py

validate:
	.venv/bin/dagster definitions validate -m data_platform.definitions
