.PHONY: install dev test test-unit test-integration test-e2e lint api ui backfill daily load-csv clean

# ── Setup ──────────────────────────────────────────────────────
install:
	pip install -e ".[dev,ui]"

# ── Dev servers ────────────────────────────────────────────────
api:
	uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload

ui:
	streamlit run ui/Home.py

dev: ## API + UI を並列起動
	@echo "Starting API on :8000 and UI on :8501"
	@uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload &
	@streamlit run ui/Home.py

# ── Tests ──────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

test-unit:
	pytest tests/unit/ -v --tb=short

test-integration:
	pytest tests/integration/ -v --tb=short

test-e2e:
	pytest tests/e2e/ -v --tb=short

test-cov:
	pytest tests/ --cov=app --cov-report=term-missing --cov-report=html

# ── Lint ───────────────────────────────────────────────────────
lint:
	ruff check app/ tests/ jobs/

lint-fix:
	ruff check --fix app/ tests/ jobs/

# ── Jobs ───────────────────────────────────────────────────────
backfill:
	python jobs/backfill_series.py --start 2020-01-01

daily:
	python jobs/daily_refresh.py

release-calendar:
	python jobs/refresh_release_calendar.py

load-csv:
	python jobs/load_csv_ohlc.py --data-dir /home/tikeda/workspace/trade/market_api/fred_api/data

load-csv-daily:
	python jobs/load_csv_ohlc.py --data-dir /home/tikeda/workspace/trade/market_api/fred_api/data --timeframes 1440

# ── Data ───────────────────────────────────────────────────────
clean-data:
	rm -rf data/raw data/normalized data/derived data/memos data/fred_fx.duckdb
	@echo "Data cleared. Run 'make backfill' to reload."

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# ── Help ───────────────────────────────────────────────────────
help:
	@echo ""
	@echo "FRED FX Research Workbench"
	@echo ""
	@echo "  make install          依存パッケージをインストール"
	@echo "  make api              FastAPI サーバーを起動 (port 8000)"
	@echo "  make ui               Streamlit UI を起動 (port 8501)"
	@echo "  make test             全テストを実行"
	@echo "  make test-unit        unit テストのみ"
	@echo "  make test-integration integration テストのみ"
	@echo "  make test-e2e         E2E テストのみ"
	@echo "  make lint             ruff でコードチェック"
	@echo "  make backfill         全 series を 2020-01-01 からバックフィル"
	@echo "  make daily            デイリー更新バッチを実行"
	@echo "  make load-csv         CSV OHLC を全足ロード + FRED 補完"
	@echo "  make load-csv-daily   CSV 日足のみロード + FRED 補完"
	@echo "  make clean-data       全データを削除 (DuckDB + Parquet)"
	@echo ""
