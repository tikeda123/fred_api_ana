# FRED FX Research Workbench 実装計画

## アーキテクチャ概要

```
FRED API → Ingestion → Raw Parquet → Normalize → DuckDB → FastAPI → Streamlit UI
```

## 技術スタック
- Backend: FastAPI + httpx (async)
- Storage: DuckDB + Parquet
- Processing: Polars
- Validation: Pandera
- UI: Streamlit + Plotly

## プロジェクトルート
`fred-fx-research/`

---

## Phase 1: 基盤構築（最優先）

### 作成ファイル
```
fred-fx-research/
  pyproject.toml
  .env.example
  app/__init__.py
  app/core/config.py          # 環境変数 (pydantic-settings)
  app/core/exceptions.py      # カスタム例外
  app/core/logging.py         # structlog 設定
  app/services/fred_client.py # FredClient (httpx async, retry, backoff)
  app/storage/duckdb.py       # DuckDB 接続管理
  sql/schema.sql              # 全テーブル定義 (7テーブル)
  app/api/main.py             # FastAPI app 初期化
  app/api/routers/health.py   # GET /health
  app/api/routers/series.py   # GET /series/search, GET /series/{id}
  app/api/routers/observations.py  # POST /observations/fetch
  app/models/api_models.py    # Pydantic request/response
  app/models/domain_models.py # ドメインモデル
  app/services/registry_service.py
  app/services/ingest_service.py
  app/storage/repositories/registry_repo.py
  app/storage/repositories/observation_repo.py
```

### FredClient メソッド
- `search_series(search_text, tag_names, limit)`
- `get_series_metadata(series_id)`
- `get_observations(series_id, start, end, units, frequency, aggregation_method, realtime_start, realtime_end)`
- `get_vintagedates(series_id)`
- `get_release_dates(start_date, end_date)`

### DuckDB テーブル
- `dim_series_registry`
- `fact_series_observations_raw`
- `fact_series_vintages`
- `fact_release_calendar`
- `fact_market_series_normalized`
- `fact_derived_factors`
- `audit_ingestion_runs`

### 完了基準
`POST /observations/fetch` で DEXJPUS の raw データが DuckDB + Parquet に保存できる

---

## Phase 2: 正規化とパネル構築

### 作成ファイル
```
  app/services/normalize_service.py
  app/services/panel_service.py
  app/storage/parquet_io.py
  app/storage/repositories/panel_repo.py
  app/api/routers/panels.py
  app/models/pandera_models.py
  ui/Home.py
  ui/pages/01_Series_Catalog.py
  ui/pages/02_Pair_Workspace.py
```

### NormalizeService
- value_raw="." → value_num=NULL
- FX quote 方向マッピング
- derived cross 生成 (EURJPY, AUDJPY, EURAUD)

### PanelService
1. pair に必要な series set を解決
2. normalized series を日付軸で as-of join (Polars lazy)
3. 欠測率計算
4. Parquet 保存

### 完了基準
UI から USDJPY を選択してパネルが3秒以内に表示

---

## Phase 3: Factor/Regime エンジン

### 作成ファイル
```
  app/services/factor_service.py
  app/services/regime_service.py
  app/api/routers/factors.py
  app/api/routers/regimes.py
  ui/pages/03_Regime_Dashboard.py
```

### FactorService
| 因子 | 計算式 |
|---|---|
| rate_spread | us_3m - jp_3m |
| yield_spread | us_10y - jp_10y |
| curve_slope | 10y - 3m |
| spot_return_Nd | N=1,5,20 |
| realized_vol_20d | rolling std |

### RegimeService
| レジーム | 条件 |
|---|---|
| risk_off | VIX > rolling_252d_p75 |
| usd_strong | DTWEXBGS zscore > 1.0 |
| carry_positive_usdjpy | us_jp_3m_spread > 0 |
| curve_inversion_us | us_curve < 0 |
| event_pressure_high | major release ±1 営業日 |

---

## Phase 4: Event Study / Memo / バッチ

### 作成ファイル
```
  app/services/memo_service.py
  app/services/freshness_service.py
  app/api/routers/releases.py
  app/api/routers/memo.py
  jobs/daily_refresh.py
  jobs/backfill_series.py
  jobs/refresh_release_calendar.py
  ui/pages/04_Event_Study.py
  ui/pages/05_Data_Quality.py
  ui/pages/06_Memo_Builder.py
```

### FreshnessService 判定ルール
| 条件 | ステータス |
|---|---|
| 日次で最終観測 > 10営業日前 | warning |
| 月次で最終観測 > 120日前 | warning |
| 四半期系列を日次モデルへ入力 | warning |
| discontinued 明記 | reject |

---

## Phase 5: Hardening

```
  tests/unit/test_normalizer.py
  tests/unit/test_factors.py
  tests/unit/test_freshness.py
  tests/integration/test_ingest.py
  tests/integration/test_vintage.py
  tests/e2e/test_ui.py
  Makefile
```

Golden dataset: DEXJPUS, DEXUSEU, DEXUSAL, DTWEXBGS

---

## 依存関係

```
Phase1 → Phase2 → Phase3 → Phase4 → Phase5
         ↑
     最小プロダクト成立
```

## 外部API
FRED のみ: https://api.stlouisfed.org/fred/
