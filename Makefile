.PHONY: install clean ingest load analyze run all test lint format dashboard reset help

help:
	@echo "Patent Intelligence Pipeline — targets:"
	@echo "  make install    - uv sync (install deps)"
	@echo "  make ingest     - copy sample TSVs into data/raw/"
	@echo "  make clean      - clean TSVs → data/clean/*.csv"
	@echo "  make load       - load clean CSVs into DuckDB"
	@echo "  make analyze    - run SQL queries → reports/"
	@echo "  make run        - full pipeline (ingest → clean → load → analyze)"
	@echo "  make dashboard  - launch Streamlit"
	@echo "  make test       - pytest"
	@echo "  make lint       - ruff check"
	@echo "  make format     - ruff format"
	@echo "  make reset      - wipe generated data and reports"

install:
	uv sync --all-extras

ingest:
	uv run patent-pipeline ingest

clean:
	uv run patent-pipeline clean

load:
	uv run patent-pipeline load

analyze:
	uv run patent-pipeline analyze

run:
	uv run patent-pipeline run-all

all: install run

dashboard:
	uv run patent-pipeline dashboard

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

reset:
	rm -rf data/raw/* data/clean/* data/warehouse/*
	rm -f reports/*.csv reports/*.json reports/figures/*.png reports/figures/*.html
