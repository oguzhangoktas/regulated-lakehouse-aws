.PHONY: up down test lint
up:
	docker compose up -d
down:
	docker compose down
test:
	python -m pytest -q
lint:
	python -m ruff check .
package:
	rm -rf dist
	python -m build --wheel -q
	$(eval BUCKET := $(shell cd infra && terraform output -raw artifacts_bucket))
	aws s3 cp --quiet dist/*.whl s3://$(BUCKET)/code/
	aws s3 cp --quiet glue/jobs/silver_exposure.py s3://$(BUCKET)/code/
	@echo "uploaded to s3://$(BUCKET)/code/"
