PYTHON ?= python

up:
	docker compose up -d postgres

down:
	docker compose down

install:
	$(PYTHON) -m pip install -r requirements.txt

seed:
	$(PYTHON) -m src.seed_legacy

run:
	$(PYTHON) -m src.main

test:
	pytest -q
