.PHONY: install dev test lint eval deploy clean

install:
	UV_NO_CONFIG=1 uv sync --extra eval

dev:
	uv run adk web app --port 8000

test:
	uv run pytest tests/unit -v

lint:
	uv run ruff check app tests

eval:
	agents-cli eval run --dataset tests/eval/datasets/eval-data.json --metrics tool_use_quality,grounding --concurrency 1

deploy:
	agents-cli deploy \
		--deployment-target agent_runtime \
		--project $${PROJECT_ID} \
		--region $${REGION:-us-central1} \
		--service-name cymbal_operations_agent \
		--service-account $${AGENT_SERVICE_ACCOUNT} \
		--no-confirm-project
