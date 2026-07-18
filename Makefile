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
	aws s3 cp --quiet --recursive glue/jobs/ s3://$(BUCKET)/code/ --exclude '*' --include '*.py'
	@echo "uploaded to s3://$(BUCKET)/code/"
