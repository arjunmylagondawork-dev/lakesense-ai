.PHONY: install dev demo pipeline dq anomalies ask app test lint format docker clean

PY ?= python

install:          ## install package + UI
	$(PY) -m pip install -e ".[app]"

dev:              ## install everything for development
	$(PY) -m pip install -e ".[app,dev]"

demo:             ## generate data, run all layers, AI reports
	lakesense all

pipeline:         ## run bronze -> silver -> gold -> publish
	lakesense run --layer all

dq:
	lakesense dq-report

anomalies:
	lakesense anomalies

ask:              ## make ask Q="top 5 customers by spend"
	lakesense ask "$(Q)"

app:
	streamlit run app/streamlit_app.py

test:
	$(PY) -m pytest

lint:
	ruff check src tests app dags

format:
	ruff format src tests app dags && ruff check --fix src tests app dags

docker:
	docker compose up --build

clean:
	rm -rf data reports/*.md reports/*.json reports/*.csv reports/*.jsonl spark-warehouse .pytest_cache
