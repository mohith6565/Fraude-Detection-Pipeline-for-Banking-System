.PHONY: install install-dev lint format precommit test run

install:
	python -m pip install --upgrade pip setuptools wheel
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

lint:
	python -m ruff check .

format:
	python -m black .

precommit:
	python -m pre_commit run --all-files

test:
	pytest -q

run:
	python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
