# Makefile

run:
	.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8081

# Test commands (실제 API 동작 테스트)
test:
	PYTHONPATH=. .venv/bin/pytest tests/ -v

test-health:
	PYTHONPATH=. .venv/bin/pytest tests/test_health.py -v

test-policy:
	PYTHONPATH=. .venv/bin/pytest tests/test_policy.py -v

test-finproduct:
	PYTHONPATH=. .venv/bin/pytest tests/test_finproduct.py -v

test-integration:
	PYTHONPATH=. .venv/bin/pytest tests/test_integration.py -v

test-coverage:
	PYTHONPATH=. .venv/bin/pytest tests/ --cov=app --cov-report=html --cov-report=term

test-verbose:
	PYTHONPATH=. .venv/bin/pytest tests/ -v -s

test-quick:
	PYTHONPATH=. .venv/bin/pytest tests/test_health.py tests/test_integration.py::TestAPIConnectivity -v

test-no-db:
	PYTHONPATH=. .venv/bin/pytest tests/test_health.py tests/test_simple.py -v

# Install dependencies
install:
	.venv/bin/pip install -r requirements.txt

# Setup test environment
setup-test:
	.venv/bin/pip install pytest pytest-asyncio pytest-cov httpx

# Clean test artifacts
clean-test:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +