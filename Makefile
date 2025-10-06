# Makefile

run:
	.venv/bin/uvicorn apps.api.main:app --reload --port 8001