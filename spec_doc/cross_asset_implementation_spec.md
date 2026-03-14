# USATECHIDXUSD クロスアセット統合 実装仕様書

## 1. 概要

既存 FRED FX Research Workbench に **USATECHIDXUSD（US Tech 100 Index）** をクロスアセット説明変数として統合する。既存の `fact_ohlc_intraday` + `csv_loader_service.py` パターンを拡張し、新スキーマ（md/ops/fact/mart）によるマルチレイヤーデータパイプラインを構築する。

### 1.1 目的

- FX ペア（USDJPY/EURUSD/AUDUSD）に対するクロスアセット説明変数の追加
- risk-on/risk-off の高頻度レジーム判定
- no-trade 条件候補の設計支援
- FRED 日次マクロ系列に対する短期センチメント補完

### 1.2 対象データ

| ファイル | パス | 行数 | 期間 | 足種 |
|----------|------|------|------|------|
| USATECHIDXUSD1.csv | data/us_index/ | 〜 | 〜 | 1分足 |
| USATECHIDXUSD5.csv | data/us_index/ | 〜 | 〜 | 5分足 |
| USATECHIDXUSD15.csv | data/us_index/ | 〜 | 〜 | 15分足 |
| USATECHIDXUSD30.csv | data/us_index/ | 〜 | 〜 | 30分足 |
| USATECHIDXUSD60.csv | data/us_index/ | 〜 | 〜 | 1時間足 |
| USATECHIDXUSD240.csv | data/us_index/ | 18,820 | 2013-05-22 〜 2026-03-13 | 4時間足 |
| USATECHIDXUSD1440.csv | data/us_index/ | 3,318 | 〜 | 日足 |

フォーマット: タブ区切り、ヘッダーなし、列順 `timestamp \t open \t high \t low \t close \t volume`

---

## 2. アーキテクチャ方針

### 2.1 制約事項（既知の学習事項）

| # | 制約 | 根拠 |
|---|------|------|
| C1 | **全書き込みは FastAPI 経由** | Streamlit 直接 DuckDB 書き込みで lock エラー発生済み（05_Data_Quality.py 修正歴あり） |
| C2 | **DuckDB single-writer** | CSV ロード時は API サーバー停止が必要、または API 経由でロード |
| C3 | **API サーバーは `--reload` で起動** | コード変更の即時反映に必要 |
| C4 | **FRED 結合は日次粒度に統一** | 4h バーを直接 FRED 日次系列に join しない |

### 2.2 データフロー

```
CSV Upload → [FastAPI] → raw (append-only)
                           ↓
                      normalized (upsert)
                           ↓
                      daily aggregation
                           ↓
                 cross-asset feature builder
                           ↓
              mart: fx_cross_asset_daily_panel
                           ↓
                    Streamlit (API 経由 read)
```

### 2.3 既存コードとの関係

| 既存コンポーネント | 新規での扱い |
|-------------------|-------------|
| `fact_ohlc_intraday` | FX ペア用は継続利用。USATECH は新スキーマ `fact.market_bars_raw` / `fact.market_bars_norm` に格納 |
| `csv_loader_service.py` | FX 用は継続。USATECH 用に `csv_ingest_service.py` を新設 |
| `factor_service.py` | 既存マクロ因子は継続。クロスアセット因子は `cross_asset_feature_service.py` に分離 |
| `panel_repo.py` | 既存パネルは継続。クロスアセットパネルは `mart.fx_cross_asset_daily_panel` として新設 |
| `ApiResponse` | 全エンドポイントで共用 |

---

## 3. DuckDB スキーマ設計

### 3.1 新規スキーマ

既存テーブル（`dim_series_registry`, `fact_ohlc_intraday` 等）はデフォルトスキーマ（main）に残す。新規テーブルのみ `md` / `ops` / `fact` / `mart` スキーマに配置する。

### 3.2 スキーマ初期化の実装方法

`sql/schema_cross_asset.sql` を新設し、`duckdb.py` の `_init_schema()` から追加ロードする。

```python
# duckdb.py の _init_schema() に追加
cross_asset_path = Path(__file__).parent.parent.parent / "sql" / "schema_cross_asset.sql"
if cross_asset_path.exists():
    sql = cross_asset_path.read_text(encoding="utf-8")
    conn.execute(sql)
```

### 3.3 テーブル定義（7テーブル）

#### 3.3.1 `md.dim_market_instrument`（商品マスタ）

```sql
CREATE SCHEMA IF NOT EXISTS md;

CREATE TABLE IF NOT EXISTS md.dim_market_instrument (
    instrument_id TEXT PRIMARY KEY,        -- 'usatechidxusd_h4'
    source_system TEXT NOT NULL,           -- 'local_csv'
    source_vendor TEXT,                    -- 'manual_upload'
    vendor_symbol TEXT NOT NULL,           -- 'USATECHIDXUSD'
    canonical_symbol TEXT NOT NULL,        -- 'USATECHIDXUSD'
    instrument_name TEXT,                  -- 'US Tech 100 Index (USD)'
    asset_class TEXT NOT NULL,             -- 'equity_index'
    venue TEXT,
    base_ccy TEXT,                         -- 'USATECHIDX'
    quote_ccy TEXT,                        -- 'USD'
    timeframe_native TEXT NOT NULL,        -- '240'
    timezone_native TEXT NOT NULL DEFAULT 'UTC',
    file_format TEXT,                      -- 'csv_tsv'
    delimiter TEXT,                        -- '\t'
    has_header BOOLEAN,                    -- false
    ts_format TEXT,                        -- '%Y-%m-%d %H:%M'
    price_decimals INTEGER,               -- 3
    volume_semantics TEXT,                 -- 'contract_volume'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

初期登録データ:

| instrument_id | timeframe_native | asset_class |
|--------------|-----------------|-------------|
| `usatechidxusd_h4` | 240 | equity_index |
| `usatechidxusd_d1` | 1440 | equity_index |

#### 3.3.2 `ops.fact_upload_audit`（取り込み監査）

アップロード単位の監査ログ。`audit_ingestion_runs`（既存）とは別に、CSV ファイル単位の詳細監査を保持。

```sql
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.fact_upload_audit (
    upload_id TEXT PRIMARY KEY,            -- 'upl_20260315_001'
    instrument_id TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,      -- 重複検知に使用
    source_file_size_bytes BIGINT,
    parser_name TEXT,                      -- 'csv_tsv_parser'
    parser_options_json TEXT,              -- JSON: delimiter, ts_format 等
    row_count_detected BIGINT,
    row_count_loaded BIGINT,
    row_count_rejected BIGINT DEFAULT 0,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,                  -- 'accepted'|'processing'|'loaded'|'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3.3.3 `fact.market_bars_raw`（生バー: append-only）

```sql
CREATE SCHEMA IF NOT EXISTS fact;

CREATE TABLE IF NOT EXISTS fact.market_bars_raw (
    upload_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    ts_utc TIMESTAMP NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    source_line_hash TEXT NOT NULL,        -- md5(row) で重複検知
    ingest_status TEXT DEFAULT 'loaded',   -- 'loaded'|'rejected'
    quality_flags TEXT,                    -- JSON: ['high_lt_low', 'zero_price'] 等
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (upload_id, source_row_number)
);
```

#### 3.3.4 `fact.market_bars_norm`（正規化バー: upsert 可能）

```sql
CREATE TABLE IF NOT EXISTS fact.market_bars_norm (
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_utc TIMESTAMP NOT NULL,
    trade_date_utc DATE NOT NULL,          -- CAST(ts_utc AS DATE)
    bar_year INTEGER NOT NULL,
    bar_month INTEGER NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    simple_ret_1bar DOUBLE,                -- close / lag(close) - 1
    log_ret_1bar DOUBLE,                   -- ln(close / lag(close))
    hl_range_pct DOUBLE,                   -- (high - low) / close
    oc_body_pct DOUBLE,                    -- abs(close - open) / open
    gap_from_prev_close_pct DOUBLE,        -- (open - prev_close) / prev_close
    h4_slot_utc TEXT,                      -- '00:00', '04:00', ...
    session_bucket TEXT,                   -- 'asia'|'europe_open'|'us_open'|'us_late'
    is_weekend_gap BOOLEAN DEFAULT FALSE,  -- gap > 8h from prev bar
    quality_status TEXT DEFAULT 'ok',
    source_upload_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_id, timeframe, ts_utc)
);
```

セッションバケット定義:

| hour (UTC) | session_bucket |
|-----------|---------------|
| 0, 4, 8 | asia |
| 12 | europe_open |
| 16 | us_open |
| 20 | us_late |
| other | other |

#### 3.3.5 `fact.market_bars_daily`（日次集約）

```sql
CREATE TABLE IF NOT EXISTS fact.market_bars_daily (
    instrument_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    timeframe_source TEXT NOT NULL,         -- '240' (H4 から集約)
    open DOUBLE NOT NULL,                  -- first(open ORDER BY ts_utc)
    high DOUBLE NOT NULL,                  -- max(high)
    low DOUBLE NOT NULL,                   -- min(low)
    close DOUBLE NOT NULL,                 -- last(close ORDER BY ts_utc)
    volume DOUBLE,                         -- sum(volume)
    bar_count INTEGER,                     -- count(*)
    simple_ret_1d DOUBLE,
    log_ret_1d DOUBLE,
    range_pct_1d DOUBLE,                   -- (high - low) / close
    gap_from_prev_close_pct DOUBLE,
    quality_status TEXT DEFAULT 'ok',
    build_id TEXT,
    built_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_id, obs_date)
);
```

#### 3.3.6 `fact.cross_asset_feature_daily`（特徴量: long-form）

```sql
CREATE TABLE IF NOT EXISTS fact.cross_asset_feature_daily (
    feature_scope TEXT NOT NULL,            -- 'instrument'|'pair'|'global'
    scope_id TEXT NOT NULL,                 -- 'usatechidxusd_h4'|'USDJPY'|'global'
    obs_date DATE NOT NULL,
    feature_group TEXT NOT NULL,            -- 'momentum'|'volatility'|'drawdown'|'correlation'
    feature_name TEXT NOT NULL,             -- 'mom_5d'|'rv_20d'|'corr_usatech_pair_20d'
    feature_horizon TEXT,                   -- '5d'|'20d'|'60d'
    feature_value DOUBLE,
    feature_value_text TEXT,                -- テキスト値（regime_label 等）
    source_instrument_id TEXT,
    source_table TEXT,
    build_id TEXT,
    built_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (feature_scope, scope_id, obs_date, feature_name, feature_horizon)
);
```

`feature_scope` / `scope_id` の設計:

| feature_scope | scope_id | 用途 |
|--------------|---------|------|
| instrument | usatechidxusd_h4 | USATECH 自体の特徴量 |
| pair | USDJPY | ペア × USATECH のクロス特徴量 |
| global | global | 全体指標 |

#### 3.3.7 `mart.fx_cross_asset_daily_panel`（UI 用横持ちパネル）

```sql
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.fx_cross_asset_daily_panel (
    pair TEXT NOT NULL,
    obs_date DATE NOT NULL,
    pair_close DOUBLE,
    pair_ret_1d DOUBLE,
    usatech_close DOUBLE,
    usatech_ret_1d DOUBLE,
    usatech_mom_5d DOUBLE,
    usatech_mom_20d DOUBLE,
    usatech_rv_5d DOUBLE,
    usatech_rv_20d DOUBLE,
    usatech_drawdown_20d DOUBLE,
    usatech_range_pct_1d DOUBLE,
    vix_close DOUBLE,
    usd_broad_close DOUBLE,
    rate_spread_3m DOUBLE,
    yield_spread_10y DOUBLE,
    event_risk_flag TEXT,
    regime_label TEXT,
    build_id TEXT,
    built_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pair, obs_date)
);
```

### 3.4 インデックス

```sql
CREATE INDEX IF NOT EXISTS idx_market_bars_norm_inst_tf_ts
    ON fact.market_bars_norm (instrument_id, timeframe, ts_utc);

CREATE INDEX IF NOT EXISTS idx_market_bars_daily_inst_date
    ON fact.market_bars_daily (instrument_id, obs_date);

CREATE INDEX IF NOT EXISTS idx_cross_asset_feature_scope_date
    ON fact.cross_asset_feature_daily (feature_scope, scope_id, obs_date);

CREATE INDEX IF NOT EXISTS idx_fx_cross_asset_panel_pair_date
    ON mart.fx_cross_asset_daily_panel (pair, obs_date);
```

---

## 4. ファイル構成（新規・変更）

```
fred-fx-research/
  app/
    api/
      routers/
        market_uploads.py        ← NEW: CSV アップロード API
        market_bars.py           ← NEW: バーデータ参照・日次再構築 API
        cross_asset.py           ← NEW: クロスアセット特徴量・パネル API
    services/
      csv_ingest_service.py      ← NEW: USATECH CSV 取り込みサービス
      market_bar_service.py      ← NEW: バー正規化・日次集約サービス
      cross_asset_feature_service.py  ← NEW: 特徴量計算サービス
      join_panel_service.py      ← NEW: FRED 結合パネル構築サービス
      validation_service.py      ← NEW: CSV バリデーションサービス
    storage/
      repositories/
        upload_repo.py           ← NEW: upload audit CRUD
        market_bar_repo.py       ← NEW: raw/norm/daily バー CRUD
        cross_asset_repo.py      ← NEW: 特徴量・パネル CRUD
    models/
      api_models.py              ← MODIFY: Pydantic モデル追加
    storage/
      duckdb.py                  ← MODIFY: schema_cross_asset.sql ロード追加
  sql/
    schema_cross_asset.sql       ← NEW: クロスアセット DDL
  ui/
    pages/
      07_Data_Upload.py          ← NEW: データアップロード画面
      08_Cross_Asset_Explorer.py ← NEW: クロスアセットエクスプローラ
      09_Lag_Correlation.py      ← NEW: 遅行相関分析画面
      10_Filter_Lab.py           ← NEW: No-trade フィルター実験画面
  jobs/
    load_usatech_csv.py          ← NEW: USATECH CSV 一括ロード
    rebuild_cross_asset_daily.py ← NEW: 日次集約再構築
    rebuild_fx_cross_asset_panel.py ← NEW: パネル再構築
  tests/
    unit/
      test_csv_ingest.py         ← NEW
      test_cross_asset_features.py ← NEW
      test_validation.py         ← NEW
    integration/
      test_cross_asset_pipeline.py ← NEW
```

---

## 5. サービス層 詳細設計

### 5.1 `csv_ingest_service.py`（CSV 取り込み）

```python
class CsvIngestService:
    """USATECH CSV の取り込みパイプライン"""

    def ingest(
        self,
        file_path: Path,
        instrument_id: str,
        upload_id: str,
        delimiter: str = "\t",
        has_header: bool = False,
        ts_format: str = "%Y-%m-%d %H:%M",
    ) -> IngestResult:
        """
        処理フロー:
        1. ファイル読み込み（Polars read_csv）
        2. バリデーション（validation_service）
        3. raw 層への append（fact.market_bars_raw）
        4. norm 層への upsert（fact.market_bars_norm）
        5. upload audit 更新（ops.fact_upload_audit）
        """

    def _compute_file_hash(self, path: Path) -> str:
        """SHA-256 チェックサム"""

    def _parse_csv(self, path: Path, ...) -> pl.DataFrame:
        """Polars で CSV パース。既存 csv_loader_service.py のパターンに準拠"""

    def _insert_raw(self, df: pl.DataFrame, upload_id: str, ...) -> int:
        """fact.market_bars_raw に append"""

    def _upsert_norm(self, df: pl.DataFrame, instrument_id: str, ...) -> int:
        """fact.market_bars_norm に delete + insert（対象期間のみ）"""
```

CSV パース仕様:

```python
df = pl.read_csv(
    path,
    separator=delimiter,
    has_header=has_header,
    new_columns=["datetime_str", "open", "high", "low", "close", "volume"],
    schema_overrides={
        "datetime_str": pl.Utf8,
        "open": pl.Float64, "high": pl.Float64,
        "low": pl.Float64, "close": pl.Float64,
        "volume": pl.Float64,
    },
)
df = df.with_columns(
    pl.col("datetime_str").str.strptime(pl.Datetime, ts_format).alias("ts_utc")
)
```

### 5.2 `validation_service.py`（バリデーション）

```python
class ValidationService:
    """CSV バーデータのバリデーション"""

    def validate_bars(self, df: pl.DataFrame) -> ValidationResult:
        """
        検査項目:
        - ts_utc の null チェック
        - high < low の矛盾
        - open/high/low/close <= 0 の無効値
        - 重複 timestamp
        - 逆順 timestamp
        - 異常 gap（前バー比 ±20% 超）
        - 異常 volume（0 未満、p99.9 超）

        Returns:
            ValidationResult(
                is_valid: bool,
                total_rows: int,
                valid_rows: int,
                rejected_rows: int,
                flags: list[QualityFlag],  # 行番号 + 理由
            )
        """
```

quality_flags JSON 例: `["high_lt_low", "zero_price", "ts_null"]`

### 5.3 `market_bar_service.py`（バー正規化・日次集約）

```python
class MarketBarService:
    """正規化済みバーの管理と日次集約"""

    def rebuild_daily(
        self,
        instrument_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> int:
        """
        fact.market_bars_norm の H4 バーから
        fact.market_bars_daily を再構築する。

        日次集約ロジック:
        - open: first(open ORDER BY ts_utc)
        - high: max(high)
        - low: min(low)
        - close: last(close ORDER BY ts_utc)
        - volume: sum(volume)
        - bar_count: count(*)
        - ret/range: window 関数で算出
        """

    def get_normalized_bars(
        self,
        instrument_id: str,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 10000,
    ) -> pl.DataFrame:
        """fact.market_bars_norm からバーを取得"""

    def get_daily_bars(
        self,
        instrument_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        """fact.market_bars_daily からデイリーバーを取得"""

    def quality_report(
        self,
        instrument_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> dict:
        """gap / 欠損 / 重複 / 異常 range のレポート"""
```

### 5.4 `cross_asset_feature_service.py`（特徴量計算）

```python
class CrossAssetFeatureService:
    """クロスアセット特徴量の計算と保存"""

    # ── instrument-level 特徴量（USATECH 自身） ──
    INSTRUMENT_FEATURES = [
        {"name": "close",            "group": "price",      "horizon": None},
        {"name": "ret_1d",           "group": "return",     "horizon": "1d"},
        {"name": "mom_5d",           "group": "momentum",   "horizon": "5d"},
        {"name": "mom_20d",          "group": "momentum",   "horizon": "20d"},
        {"name": "rv_5d",            "group": "volatility", "horizon": "5d"},
        {"name": "rv_20d",           "group": "volatility", "horizon": "20d"},
        {"name": "drawdown_20d",     "group": "drawdown",   "horizon": "20d"},
        {"name": "range_pct_1d",     "group": "range",      "horizon": "1d"},
        {"name": "weekend_gap_pct",  "group": "range",      "horizon": None},
    ]

    # ── pair-level 特徴量（FX × USATECH） ──
    PAIR_FEATURES = [
        {"name": "corr_usatech_pair_20d",    "group": "correlation", "horizon": "20d"},
        {"name": "beta_usatech_pair_60d",    "group": "correlation", "horizon": "60d"},
        {"name": "lagcorr_usatech_pair_l1_20d", "group": "lag_correlation", "horizon": "20d"},
        {"name": "lagcorr_usatech_pair_l2_20d", "group": "lag_correlation", "horizon": "20d"},
        {"name": "risk_filter_flag",         "group": "filter",      "horizon": None},
    ]

    def rebuild_instrument_features(
        self,
        instrument_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> int:
        """
        fact.market_bars_daily から instrument-level 特徴量を計算し
        fact.cross_asset_feature_daily に long-form で保存。

        計算式:
        - ret_1d:        close / lag(close, 1) - 1
        - mom_5d:        close / lag(close, 5) - 1
        - mom_20d:       close / lag(close, 20) - 1
        - rv_5d:         rolling_std(ret_1d, 5) * sqrt(252)
        - rv_20d:        rolling_std(ret_1d, 20) * sqrt(252)
        - drawdown_20d:  close / rolling_max(close, 20) - 1
        - range_pct_1d:  (high - low) / close
        - weekend_gap_pct: (open - lag(close, 1)) / lag(close, 1) ※ gap > 8h
        """

    def rebuild_pair_features(
        self,
        pairs: list[str],
        source_instrument_id: str,
        start: date | None = None,
        end: date | None = None,
    ) -> int:
        """
        pair-level 特徴量を計算。
        fact.market_bars_daily (USATECH) と
        fact_market_series_normalized (FX spot) を日次で結合。

        計算式:
        - corr_20d:   rolling_corr(usatech_ret_1d, pair_ret_1d, 20)
        - beta_60d:   rolling_cov(usatech, pair, 60) / rolling_var(usatech, 60)
        - lagcorr_l1: rolling_corr(usatech_ret_1d.shift(1), pair_ret_1d, 20)
        - lagcorr_l2: rolling_corr(usatech_ret_1d.shift(2), pair_ret_1d, 20)
        - risk_filter_flag: ルールベース（§5.5 参照）
        """
```

### 5.5 No-Trade フィルターロジック（初期版）

ルールベース。いずれか **2つ以上** 該当で `risk_filter_flag = 'avoid'`。

| 条件 | 閾値 | 意味 |
|------|------|------|
| `usatech_drawdown_20d <= -0.08` | -8% | 株式指数が大幅下落中 |
| `usatech_rv_20d >= percentile(80)` | 動的 | ボラが異常に高い |
| `vix_close >= percentile(80)` | 動的 | 恐怖指数が高い |
| `usd_broad_close_20d_z >= +1.0` | z-score | USD が異常に強い |

### 5.6 `join_panel_service.py`（パネル構築）

```python
class JoinPanelService:
    """mart.fx_cross_asset_daily_panel の構築"""

    def build_panel(
        self,
        pair: str,
        start: date | None = None,
        end: date | None = None,
    ) -> int:
        """
        データソースの結合:
        1. FX spot close  ← fact_market_series_normalized (既存)
        2. FX spot ret_1d ← 上記から計算
        3. USATECH close/ret/mom/rv/dd/range ← fact.cross_asset_feature_daily (instrument)
        4. VIX close      ← fact_market_series_normalized (VIXCLS)
        5. USD broad      ← fact_market_series_normalized (DTWEXBGS)
        6. rate_spread_3m ← fact_derived_factors (既存)
        7. yield_spread_10y ← fact_derived_factors (既存)
        8. risk_filter_flag ← fact.cross_asset_feature_daily (pair)
        9. regime_label   ← fact_derived_factors (既存) or 新規計算

        結合キー: obs_date (LEFT JOIN on FX spot)
        保存先:   mart.fx_cross_asset_daily_panel (delete + insert)
        """
```

---

## 6. API エンドポイント詳細

### 6.1 ルーター登録（app/api/main.py 変更）

```python
from app.api.routers import market_uploads, market_bars, cross_asset

app.include_router(market_uploads.router, prefix="/api/v1", tags=["market-uploads"])
app.include_router(market_bars.router, prefix="/api/v1", tags=["market-bars"])
app.include_router(cross_asset.router, prefix="/api/v1", tags=["cross-asset"])
```

### 6.2 エンドポイント一覧

| # | Method | Path | Router | 目的 |
|---|--------|------|--------|------|
| 1 | POST | `/api/v1/market/uploads` | market_uploads | CSV アップロード＆取り込み |
| 2 | GET | `/api/v1/market/uploads/{upload_id}` | market_uploads | アップロード状態確認 |
| 3 | GET | `/api/v1/market/bars` | market_bars | 正規化バー取得 |
| 4 | POST | `/api/v1/market/bars/rebuild-daily` | market_bars | 日次集約再構築 |
| 5 | POST | `/api/v1/cross-asset/features/rebuild` | cross_asset | 特徴量再計算 |
| 6 | GET | `/api/v1/cross-asset/features` | cross_asset | 特徴量取得 (long-form) |
| 7 | GET | `/api/v1/panels/fx-crossasset` | cross_asset | パネル取得 (wide-form) |
| 8 | GET | `/api/v1/quality/market-report` | market_bars | 品質レポート |

### 6.3 エンドポイント詳細

#### EP1: `POST /api/v1/market/uploads`

Content-Type: `multipart/form-data`

| フィールド | 型 | デフォルト | 説明 |
|-----------|------|---------|------|
| file | UploadFile | (必須) | CSV/TSV ファイル |
| vendor_symbol | str | (必須) | 'USATECHIDXUSD' |
| canonical_symbol | str | (必須) | 'USATECHIDXUSD' |
| instrument_name | str? | null | 'US Tech 100 Index (USD)' |
| asset_class | str | 'equity_index' | |
| base_ccy | str | 'USATECHIDX' | |
| quote_ccy | str | 'USD' | |
| timeframe | str | '240' | |
| delimiter | str | '\t' | |
| has_header | bool | false | |
| ts_format | str | '%Y-%m-%d %H:%M' | |

Response 202:
```json
{
  "status": "ok",
  "data": {
    "upload_id": "upl_20260315_001",
    "instrument_id": "usatechidxusd_h4",
    "status": "accepted",
    "next": "/api/v1/market/uploads/upl_20260315_001"
  }
}
```

内部処理:
1. `md.dim_market_instrument` に upsert
2. アップロードファイルを `data/raw/uploads/` に保存
3. `ops.fact_upload_audit` に `accepted` で作成
4. `BackgroundTasks` で `csv_ingest_service.ingest()` を実行
5. 完了後 audit を `loaded` / `failed` に更新

#### EP2: `GET /api/v1/market/uploads/{upload_id}`

Response 200:
```json
{
  "status": "ok",
  "data": {
    "upload_id": "upl_20260315_001",
    "instrument_id": "usatechidxusd_h4",
    "status": "loaded",
    "row_count_detected": 18820,
    "row_count_loaded": 18820,
    "row_count_rejected": 0,
    "started_at": "2026-03-15T10:00:01Z",
    "finished_at": "2026-03-15T10:00:03Z",
    "error_message": null
  }
}
```

#### EP3: `GET /api/v1/market/bars`

Query params: `instrument_id`, `timeframe`, `start`, `end`, `limit` (default=10000)

#### EP4: `POST /api/v1/market/bars/rebuild-daily`

Request JSON:
```json
{
  "instrument_id": "usatechidxusd_h4",
  "start": "2013-05-22",
  "end": "2026-03-13"
}
```

#### EP5: `POST /api/v1/cross-asset/features/rebuild`

Request JSON:
```json
{
  "source_instrument_id": "usatechidxusd_h4",
  "pairs": ["USDJPY", "EURUSD", "AUDUSD"],
  "start": "2013-05-22",
  "end": "2026-03-13",
  "feature_set": [
    "ret_1d", "mom_5d", "mom_20d",
    "rv_5d", "rv_20d", "drawdown_20d",
    "corr_20d", "beta_60d",
    "lagcorr_l1_20d", "lagcorr_l2_20d"
  ]
}
```

#### EP6: `GET /api/v1/cross-asset/features`

Query params: `feature_scope`, `scope_id`, `feature_names` (comma-separated), `start`, `end`

#### EP7: `GET /api/v1/panels/fx-crossasset`

Query params: `pair`, `start`, `end`, `include_usatech` (bool), `include_fred` (bool)

#### EP8: `GET /api/v1/quality/market-report`

Query params: `instrument_id`, `start`, `end`

---

## 7. Pydantic モデル追加（app/models/api_models.py）

```python
from typing import Literal

class MarketUploadAccepted(BaseModel):
    upload_id: str
    instrument_id: str
    status: Literal["accepted", "processing", "loaded", "failed"]
    next: str

class UploadStatusResponse(BaseModel):
    upload_id: str
    instrument_id: str
    status: str
    row_count_detected: int | None = None
    row_count_loaded: int | None = None
    row_count_rejected: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None

class DailyRebuildRequest(BaseModel):
    instrument_id: str
    start: date | None = None
    end: date | None = None

class CrossAssetFeatureBuildRequest(BaseModel):
    source_instrument_id: str
    pairs: list[str]
    start: date | None = None
    end: date | None = None
    feature_set: list[str]

class FxCrossAssetPanelRow(BaseModel):
    pair: str
    obs_date: date
    pair_close: float | None = None
    pair_ret_1d: float | None = None
    usatech_close: float | None = None
    usatech_ret_1d: float | None = None
    usatech_mom_5d: float | None = None
    usatech_mom_20d: float | None = None
    usatech_rv_5d: float | None = None
    usatech_rv_20d: float | None = None
    usatech_drawdown_20d: float | None = None
    usatech_range_pct_1d: float | None = None
    vix_close: float | None = None
    usd_broad_close: float | None = None
    rate_spread_3m: float | None = None
    yield_spread_10y: float | None = None
    event_risk_flag: str | None = None
    regime_label: str | None = None
```

---

## 8. Repository 層詳細

### 8.1 `upload_repo.py`

```python
class UploadRepo:
    def create_audit(self, upload_id, instrument_id, file_name, file_hash, file_size) -> None
    def update_status(self, upload_id, status, row_counts=None, error=None) -> None
    def get_audit(self, upload_id) -> dict | None
    def list_recent(self, limit=20) -> list[dict]
```

### 8.2 `market_bar_repo.py`

```python
class MarketBarRepo:
    def insert_raw(self, df: pl.DataFrame) -> int
    def upsert_norm(self, instrument_id, timeframe, df: pl.DataFrame) -> int
    def upsert_daily(self, instrument_id, df: pl.DataFrame) -> int
    def get_norm_bars(self, instrument_id, timeframe, start, end, limit) -> list[dict]
    def get_daily_bars(self, instrument_id, start, end) -> list[dict]
    def count_by_instrument(self, instrument_id) -> dict  # raw/norm/daily 各行数
```

### 8.3 `cross_asset_repo.py`

```python
class CrossAssetRepo:
    def upsert_features(self, df: pl.DataFrame) -> int
    def get_features(self, scope, scope_id, feature_names, start, end) -> list[dict]
    def upsert_panel(self, pair, df: pl.DataFrame) -> int
    def get_panel(self, pair, start, end) -> list[dict]
    def upsert_instrument(self, instrument_id, metadata: dict) -> None
```

---

## 9. Streamlit UI 画面仕様

### 9.1 共通方針

- 全データ取得は API 経由（DuckDB 直接アクセス禁止）
- `st.cache_data` で API GET 結果をキャッシュ
- JST 表示は既存 `tz_utils.py` を使用
- `st.session_state` で pair/date range を共有

### 9.2 `07_Data_Upload.py`

**目的**: CSV/TSV をアップロードし、取り込み状況を確認

**UI 構成**:
1. `st.file_uploader` — CSV/TSV ファイル選択
2. Parser preset selector — USATECHIDXUSD がデフォルト
3. 先頭 20 行プレビュー（`st.dataframe`）
4. 列マッピング表示
5. Upload 実行ボタン → `POST /api/v1/market/uploads`
6. Status poller — `GET /api/v1/market/uploads/{upload_id}` を定期確認
7. 最近のアップロード一覧テーブル

**表示項目**:
- file_name, detected_rows, loaded_rows, rejected_rows
- min/max timestamp, duplicate count, status

### 9.3 `08_Cross_Asset_Explorer.py`

**目的**: FX ペアと USATECH を重ねてレジーム/リスク状況を確認

**UI 構成**:
1. Pair selector (USDJPY/EURUSD/AUDUSD)
2. Date range selector
3. Overlay toggles: USATECH, VIX, USD Broad, 3M spread, 10Y spread
4. Normalization mode: raw / indexed=100 / z-score

**チャート**:
- メイン: pair close + USATECH indexed (dual y-axis)
- サブ1: USATECH ret / drawdown
- サブ2: USATECH realized vol (5d, 20d)
- Event shading (optional)

データ取得: `GET /api/v1/panels/fx-crossasset?pair={pair}&start={start}&end={end}`

### 9.4 `09_Lag_Correlation.py`

**目的**: USATECH と FX の遅行相関・ローリング β を分析

**UI 構成**:
1. Pair selector
2. Lag range selector (-5d ~ +5d)
3. Rolling window selector (20d / 60d / 120d)
4. Return basis selector (4h / 1d)

**チャート**:
- Lag-correlation bar chart（各ラグの相関係数）
- Rolling correlation line chart
- Rolling beta line chart
- Scatter plot + regression line

データ取得: `GET /api/v1/cross-asset/features?feature_scope=pair&scope_id={pair}&feature_names=corr_usatech_pair_20d,beta_usatech_pair_60d,...`

### 9.5 `10_Filter_Lab.py`

**目的**: No-trade 条件を対話的に設計・検証

**UI 構成**:
1. Threshold sliders:
   - `usatech_drawdown_20d` (-15% ~ 0%)
   - `usatech_rv_20d` (0% ~ 50%)
   - `VIX` (10 ~ 60)
   - `usd_broad_zscore` (-2 ~ +3)
2. Boolean expression builder (AND/OR)
3. Hit-rate summary metrics
4. Excluded-days count / percentage

**出力**:
- 採用ルール表示
- 該当日一覧（`st.dataframe`）
- ルール JSON エクスポート（`st.download_button`）

---

## 10. バッチジョブ

### 10.1 `jobs/load_usatech_csv.py`

```python
"""
USATECH CSV 一括ロード
Usage: python jobs/load_usatech_csv.py --data-dir data/us_index --timeframes 240,1440
注意: API サーバー停止時に実行すること（DuckDB single-writer 制約）
"""
```

処理:
1. `data/us_index/USATECHIDXUSD{tf}.csv` を検索
2. `csv_ingest_service.ingest()` で各ファイルをロード
3. `market_bar_service.rebuild_daily()` で日次集約
4. ロード結果サマリを出力

### 10.2 `jobs/rebuild_cross_asset_daily.py`

```python
"""
クロスアセット特徴量・パネルの再構築
Usage: python jobs/rebuild_cross_asset_daily.py --pairs USDJPY,EURUSD,AUDUSD
"""
```

処理:
1. `cross_asset_feature_service.rebuild_instrument_features()`
2. `cross_asset_feature_service.rebuild_pair_features()`
3. `join_panel_service.build_panel()` × 各ペア

---

## 11. テスト計画

### 11.1 ユニットテスト

| テストファイル | テスト内容 |
|-------------|----------|
| `test_csv_ingest.py` | CSV パース、ハッシュ計算、raw 挿入、重複検知 |
| `test_validation.py` | high<low 検出、null ts 検出、異常 gap 検出、逆順 ts |
| `test_cross_asset_features.py` | mom_5d/rv_20d/drawdown_20d/corr/beta 計算値の手計算比較 |

### 11.2 統合テスト

| テストファイル | テスト内容 |
|-------------|----------|
| `test_cross_asset_pipeline.py` | CSV → raw → norm → daily → feature → panel の一気通貫 |

### 11.3 受け入れ基準

| # | 基準 | 検証方法 |
|---|------|---------|
| A1 | USATECHIDXUSD240.csv を 1 回の upload で取り込める | EP1 → EP2 で status=loaded |
| A2 | upload audit に件数と status が残る | EP2 レスポンス確認 |
| A3 | 重複 timestamp は reject or warning | 同一ファイル 2 回目投入 |
| A4 | H4 → daily OHLC の再現 | daily 行数 ≈ 3,318 行 |
| A5 | ret_1d, mom_5d, rv_20d, drawdown_20d が生成される | EP6 で確認 |
| A6 | 3 ペアの panel が生成される | EP7 で確認 |
| A7 | upload → status 確認が UI で完結 | 07_Data_Upload |
| A8 | pair overlay が表示される | 08_Cross_Asset_Explorer |
| A9 | lag correlation が動作する | 09_Lag_Correlation |
| A10 | filter lab が動作する | 10_Filter_Lab |

---

## 12. 実装フェーズ

### Phase 1: スキーマ＆取り込み基盤（優先度: 最高）

| 順序 | タスク | 成果物 |
|------|--------|--------|
| 1-1 | `sql/schema_cross_asset.sql` 作成 | DDL ファイル |
| 1-2 | `duckdb.py` にスキーマロード追加 | 起動時に自動作成 |
| 1-3 | `validation_service.py` 実装 | バリデーション |
| 1-4 | `upload_repo.py` 実装 | audit CRUD |
| 1-5 | `market_bar_repo.py` 実装 | raw/norm/daily CRUD |
| 1-6 | `csv_ingest_service.py` 実装 | CSV → raw → norm |
| 1-7 | `market_bar_service.py` 実装 | 日次集約 |
| 1-8 | `market_uploads.py` ルーター | EP1, EP2 |
| 1-9 | `market_bars.py` ルーター | EP3, EP4, EP8 |
| 1-10 | `jobs/load_usatech_csv.py` | 初期データ投入 |
| 1-11 | ユニットテスト | test_csv_ingest, test_validation |

### Phase 2: 特徴量＆パネル（優先度: 高）

| 順序 | タスク | 成果物 |
|------|--------|--------|
| 2-1 | `cross_asset_repo.py` 実装 | 特徴量・パネル CRUD |
| 2-2 | `cross_asset_feature_service.py` 実装 | instrument + pair 特徴量 |
| 2-3 | `join_panel_service.py` 実装 | mart パネル構築 |
| 2-4 | `cross_asset.py` ルーター | EP5, EP6, EP7 |
| 2-5 | `api_models.py` にモデル追加 | Pydantic モデル |
| 2-6 | `jobs/rebuild_cross_asset_daily.py` | バッチ再構築 |
| 2-7 | ユニット + 統合テスト | test_cross_asset_features, test_pipeline |

### Phase 3: Streamlit UI（優先度: 中）

| 順序 | タスク | 成果物 |
|------|--------|--------|
| 3-1 | `07_Data_Upload.py` | アップロード画面 |
| 3-2 | `08_Cross_Asset_Explorer.py` | オーバーレイ分析 |
| 3-3 | `09_Lag_Correlation.py` | 遅行相関分析 |
| 3-4 | `10_Filter_Lab.py` | フィルター実験 |

### Phase 4: 品質＆運用（優先度: 低）

| 順序 | タスク | 成果物 |
|------|--------|--------|
| 4-1 | Parquet export 機能 | アーカイブ出力 |
| 4-2 | 05_Data_Quality.py に USATECH 品質表示追加 | UI 拡張 |
| 4-3 | 運用ドキュメント更新 | docs/ |

---

## 13. 運用ルール

1. Streamlit は read-heavy、write は API 経由のみ（**C1** 厳守）
2. Upload は audit を必須化（traceability）
3. `fact.market_bars_raw` は append-only、上書き禁止
4. `fact.market_bars_norm` / `fact.market_bars_daily` / `mart.*` は再構築可能
5. クロスアセット特徴量は必ず `build_id` と `built_at` を持つ
6. FX/FRED join は日次粒度で行う（**C4** 厳守）
7. 初期データ投入時は API サーバーを停止してからジョブ実行（**C2** 対応）
8. 特徴量保存は long-form（`fact.cross_asset_feature_daily`）を正、mart で横持ち化
