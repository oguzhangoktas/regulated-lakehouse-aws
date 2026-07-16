.PHONY: up down test lint
up:
	docker compose up -d
down:
	docker compose down
test:
	python -m pytest -q
lint:
	python -m ruff check .