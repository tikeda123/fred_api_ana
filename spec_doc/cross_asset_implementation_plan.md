# USATECHIDXUSD クロスアセット統合 実装プラン

## 全体構成

4 Phase / 28 Step。各 Step は 1 コミット単位を想定。
依存関係は上から下へ流れる（後続 Step は先行 Step の成果物に依存）。

---

## Phase 1: スキーマ＆取り込み基盤

**ゴール**: CSV → raw → norm → daily のパイプラインを動かし、USATECHIDXUSD の H4/D1 データを DuckDB に格納する。

### Step 1-1: DDL ファイル作成

**ファイル**: `sql/schema_cross_asset.sql` (NEW)

- `schema_cross_asset.sql` を spec_doc からコピーではなく、プロジェクト用に作成
- 4 スキーマ: `md`, `ops`, `fact`, `mart`
- 7 テーブル: `md.dim_market_instrument`, `ops.fact_upload_audit`, `fact.market_bars_raw`, `fact.market_bars_norm`, `fact.market_bars_daily`, `fact.cross_asset_feature_daily`, `mart.fx_cross_asset_daily_panel`
- 4 インデックス

**検証**: `duckdb` CLI で DDL を直接実行し、テーブルが作成されることを確認

---

### Step 1-2: DuckDB スキーマ自動初期化

**ファイル**: `app/storage/duckdb.py` (MODIFY)

変更内容:
- `_init_schema()` に `sql/schema_cross_asset.sql` のロードを追加
- `schema.sql` の後に実行（既存テーブルに影響なし）

```python
# _init_schema() 末尾に追加
cross_asset_path = Path(__file__).parent.parent.parent / "sql" / "schema_cross_asset.sql"
if cross_asset_path.exists():
    sql_ca = cross_asset_path.read_text(encoding="utf-8")
    conn.execute(sql_ca)
```

**検証**: API サーバー起動後、DuckDB に 4 スキーマ・7 テーブルが存在することを確認

**依存**: Step 1-1

---

### Step 1-3: Pydantic モデル追加

**ファイル**: `app/models/api_models.py` (MODIFY)

追加モデル:
- `MarketUploadAccepted` — upload レスポンス
- `UploadStatusResponse` — upload 状態
- `DailyRebuildRequest` — 日次再構築リクエスト
- `CrossAssetFeatureBuildRequest` — 特徴量計算リクエスト
- `FxCrossAssetPanelRow` — パネル行

**検証**: `python -c "from app.models.api_models import MarketUploadAccepted"` が成功

**依存**: なし

---

### Step 1-4: バリデーションサービス

**ファイル**: `app/services/validation_service.py` (NEW)

実装内容:
- `validate_bars(df: pl.DataFrame) -> ValidationResult` 関数
- 検査項目: ts_utc null, high < low, price <= 0, 重複 ts, 逆順 ts, 異常 gap (±20%), 異常 volume
- `ValidationResult` dataclass: `is_valid`, `total_rows`, `valid_rows`, `rejected_rows`, `flags`
- `QualityFlag` dataclass: `row_number`, `reason`, `details`

パターン: 既存 `freshness_service.py` のモジュール関数スタイルに合わせる（クラスではなく関数）

**検証**: ユニットテスト（Step 1-11）

**依存**: なし

---

### Step 1-5: Upload リポジトリ

**ファイル**: `app/storage/repositories/upload_repo.py` (NEW)

実装内容:
- `create_audit(upload_id, instrument_id, source_system, file_name, file_hash, file_size, parser_options) -> None`
- `update_status(upload_id, status, row_count_detected=None, row_count_loaded=None, row_count_rejected=None, error_message=None, started_at=None, finished_at=None) -> None`
- `get_audit(upload_id) -> dict | None`
- `list_recent(limit=20) -> list[dict]`
- テーブル: `ops.fact_upload_audit`

パターン: 既存 `registry_repo.py` に合わせ、`get_connection()` で conn 取得

**検証**: ユニットテスト（Step 1-11）

**依存**: Step 1-2

---

### Step 1-6: Market Bar リポジトリ

**ファイル**: `app/storage/repositories/market_bar_repo.py` (NEW)

実装内容:
- `insert_raw(df: pl.DataFrame) -> int` — `fact.market_bars_raw` に append（Arrow 経由）
- `upsert_norm(instrument_id, timeframe, min_ts, max_ts, df: pl.DataFrame) -> int` — delete + insert
- `upsert_daily(instrument_id, min_date, max_date, df: pl.DataFrame) -> int` — delete + insert
- `get_norm_bars(instrument_id, timeframe, start, end, limit) -> list[dict]`
- `get_daily_bars(instrument_id, start, end) -> list[dict]`
- `count_by_instrument(instrument_id) -> dict` — raw/norm/daily 各行数

パターン: 既存 `observation_repo.py` の Arrow 登録 + SQL パターンに合わせる

```python
conn.register("_df", df.to_arrow())
conn.execute("INSERT INTO fact.market_bars_raw SELECT * FROM _df")
conn.unregister("_df")
```

**検証**: ユニットテスト（Step 1-11）

**依存**: Step 1-2

---

### Step 1-7: CSV 取り込みサービス

**ファイル**: `app/services/csv_ingest_service.py` (NEW)

実装内容:
- `ingest(file_path, instrument_id, upload_id, timeframe, delimiter, has_header, ts_format) -> dict`
  - 返り値: `{"row_count_detected": int, "row_count_loaded": int, "row_count_rejected": int}`

処理フロー:
1. SHA-256 ハッシュ計算
2. Polars `read_csv` でパース（既存 `csv_loader_service.py` と同じパターン）
3. `validation_service.validate_bars()` でバリデーション
4. `market_bar_repo.insert_raw()` で raw に append
5. 正規化列計算（ret, range, session_bucket, is_weekend_gap 等）→ Polars で算出
6. `market_bar_repo.upsert_norm()` で norm に upsert
7. `upload_repo.update_status()` で audit 更新

正規化計算の詳細（Polars）:
```python
df = df.with_columns([
    (pl.col("close") / pl.col("close").shift(1) - 1).alias("simple_ret_1bar"),
    (pl.col("close") / pl.col("close").shift(1)).log().alias("log_ret_1bar"),
    ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("hl_range_pct"),
    ((pl.col("close") - pl.col("open")).abs() / pl.col("open")).alias("oc_body_pct"),
    ((pl.col("open") - pl.col("close").shift(1)) / pl.col("close").shift(1)).alias("gap_from_prev_close_pct"),
    pl.col("ts_utc").dt.strftime("%H:%M").alias("h4_slot_utc"),
    pl.col("ts_utc").cast(pl.Date).alias("trade_date_utc"),
    pl.col("ts_utc").dt.year().alias("bar_year"),
    pl.col("ts_utc").dt.month().alias("bar_month"),
])
```

セッションバケット:
```python
hour = pl.col("ts_utc").dt.hour()
df = df.with_columns(
    pl.when(hour.is_in([0, 4, 8])).then(pl.lit("asia"))
    .when(hour == 12).then(pl.lit("europe_open"))
    .when(hour == 16).then(pl.lit("us_open"))
    .when(hour == 20).then(pl.lit("us_late"))
    .otherwise(pl.lit("other"))
    .alias("session_bucket")
)
```

**検証**: ユニットテスト（Step 1-11）

**依存**: Step 1-4, 1-5, 1-6

---

### Step 1-8: Market Bar サービス

**ファイル**: `app/services/market_bar_service.py` (NEW)

実装内容:
- `rebuild_daily(instrument_id, start=None, end=None) -> int`
  - `fact.market_bars_norm` の H4 バーを trade_date_utc で GROUP BY
  - open: first(ORDER BY ts_utc), high: max, low: min, close: last(ORDER BY ts_utc)
  - volume: sum, bar_count: count
  - ret/range は window 関数（Polars）
  - `market_bar_repo.upsert_daily()` で保存
- `get_normalized_bars(instrument_id, timeframe, start, end, limit) -> list[dict]`
- `get_daily_bars(instrument_id, start, end) -> list[dict]`
- `quality_report(instrument_id, start, end) -> dict`
  - gap 検出（連続バー間の時間差異常）
  - 欠損日検出（営業日カレンダー対比）
  - 重複 timestamp 検出
  - 異常 range 検出

日次集約 SQL（DuckDB で直接実行する方式も可、Polars でも可）:
```sql
WITH d AS (
    SELECT instrument_id, trade_date_utc AS obs_date,
           first(open ORDER BY ts_utc) AS open,
           max(high) AS high, min(low) AS low,
           last(close ORDER BY ts_utc) AS close,
           sum(volume) AS volume, count(*) AS bar_count
    FROM fact.market_bars_norm
    WHERE instrument_id = ? AND timeframe = '240'
      AND trade_date_utc BETWEEN ? AND ?
    GROUP BY 1, 2
)
SELECT *, close / lag(close) OVER (ORDER BY obs_date) - 1 AS simple_ret_1d,
       ln(close / lag(close) OVER (ORDER BY obs_date)) AS log_ret_1d,
       (high - low) / nullif(close, 0) AS range_pct_1d
FROM d ORDER BY obs_date
```

**検証**: ユニットテスト（Step 1-11）

**依存**: Step 1-6

---

### Step 1-9: Market Uploads ルーター

**ファイル**: `app/api/routers/market_uploads.py` (NEW)

エンドポイント:
- `POST /market/uploads` (EP1) — multipart/form-data で CSV 受信
  - ファイルを `data/raw/uploads/` に保存
  - `upload_repo.create_audit()` で audit 作成
  - `BackgroundTasks` で `csv_ingest_service.ingest()` 実行
  - Response 202: `ApiResponse(data=MarketUploadAccepted(...))`
- `GET /market/uploads/{upload_id}` (EP2) — 状態確認
  - `upload_repo.get_audit()` で取得
  - Response 200: `ApiResponse(data=UploadStatusResponse(...))`

**ファイル**: `app/api/main.py` (MODIFY)
- `from app.api.routers import market_uploads` 追加
- `app.include_router(market_uploads.router, prefix="/api/v1", tags=["market-uploads"])` 追加

**検証**: curl で EP1, EP2 を手動テスト

**依存**: Step 1-3, 1-5, 1-7

---

### Step 1-10: Market Bars ルーター

**ファイル**: `app/api/routers/market_bars.py` (NEW)

エンドポイント:
- `GET /market/bars` (EP3) — 正規化バー取得
  - Query: instrument_id, timeframe, start, end, limit
- `POST /market/bars/rebuild-daily` (EP4) — 日次集約再構築
  - Body: `DailyRebuildRequest`
  - `market_bar_service.rebuild_daily()` 呼び出し
- `GET /quality/market-report` (EP8) — 品質レポート
  - Query: instrument_id, start, end

**ファイル**: `app/api/main.py` (MODIFY)
- `from app.api.routers import market_bars` 追加
- `app.include_router(market_bars.router, prefix="/api/v1", tags=["market-bars"])` 追加

**検証**: curl で EP3, EP4, EP8 を手動テスト

**依存**: Step 1-8

---

### Step 1-11: Phase 1 ユニットテスト

**ファイル**: `tests/unit/test_validation.py` (NEW)

テストケース:
- `test_valid_bars` — 正常データが is_valid=True
- `test_high_lt_low` — high < low 検出
- `test_null_timestamp` — ts_utc null 検出
- `test_zero_price` — price <= 0 検出
- `test_duplicate_timestamp` — 重複 ts 検出
- `test_reverse_order` — 逆順 ts 検出
- `test_abnormal_gap` — ±20% 超の gap 検出

**ファイル**: `tests/unit/test_csv_ingest.py` (NEW)

テストケース:
- `test_parse_csv` — タブ区切り・ヘッダーなし CSV パース
- `test_hash_computation` — SHA-256 ハッシュ一致
- `test_normalization_columns` — 正規化列（ret, range, session_bucket）の算出
- `test_ingest_end_to_end` — mock DB で raw + norm 挿入（`unittest.mock.patch`）
- `test_empty_csv` — 空ファイルの graceful 処理

テスト方式: `unittest.mock.patch` で `get_connection()` をモック（既存 `test_factors.py` と同じパターン）

**検証**: `pytest tests/unit/test_validation.py tests/unit/test_csv_ingest.py -v`

**依存**: Step 1-4, 1-7

---

### Step 1-12: 初期データ投入ジョブ

**ファイル**: `jobs/load_usatech_csv.py` (NEW)

```python
"""
USATECH CSV 一括ロード
Usage: python jobs/load_usatech_csv.py --data-dir /path/to/data/us_index --timeframes 240,1440
注意: API サーバー停止時に実行（DuckDB single-writer）
"""
```

処理:
1. argparse で `--data-dir`, `--timeframes` を受け取る
2. `USATECHIDXUSD{tf}.csv` を検索
3. instrument_id 決定: `usatechidxusd_h4` (240) / `usatechidxusd_d1` (1440)
4. `md.dim_market_instrument` に初期レコード upsert
5. `csv_ingest_service.ingest()` で各ファイルをロード
6. `market_bar_service.rebuild_daily()` で H4 → daily 集約
7. サマリ出力（ロード行数、所要時間）

実行手順:
```bash
# API サーバー停止
# ジョブ実行
python jobs/load_usatech_csv.py \
  --data-dir /home/tikeda/workspace/trade/market_api/fred_api/data/us_index \
  --timeframes 240,1440
# API サーバー再起動
```

**検証**: 実行後 DuckDB で `SELECT count(*) FROM fact.market_bars_raw` / `fact.market_bars_norm` / `fact.market_bars_daily` を確認

**依存**: Step 1-7, 1-8

---

### Phase 1 完了チェックポイント

- [ ] API 起動で 4 スキーマ・7 テーブルが自動作成される
- [ ] `POST /api/v1/market/uploads` で CSV をアップロードできる
- [ ] `GET /api/v1/market/uploads/{id}` で status=loaded が返る
- [ ] `GET /api/v1/market/bars?instrument_id=usatechidxusd_h4&timeframe=240` でバーが返る
- [ ] `POST /api/v1/market/bars/rebuild-daily` で日次集約が再構築される
- [ ] `fact.market_bars_raw` に 18,820 行（H4）、`fact.market_bars_daily` に ~3,318 行
- [ ] `pytest tests/unit/test_validation.py tests/unit/test_csv_ingest.py -v` 全 pass

---

## Phase 2: 特徴量＆パネル

**ゴール**: USATECH の instrument-level 特徴量と FX pair-level 特徴量を計算し、mart パネルを構築する。

### Step 2-1: Cross Asset リポジトリ

**ファイル**: `app/storage/repositories/cross_asset_repo.py` (NEW)

実装内容:
- `upsert_instrument(instrument_id, metadata: dict) -> None` — `md.dim_market_instrument` upsert
- `upsert_features(scope, scope_id, min_date, max_date, df: pl.DataFrame) -> int` — `fact.cross_asset_feature_daily` delete + insert
- `get_features(scope, scope_id, feature_names, start, end) -> list[dict]` — long-form 取得
- `get_features_pivot(scope, scope_id, feature_names, start, end) -> list[dict]` — wide-form 取得（UI 用）
- `upsert_panel(pair, min_date, max_date, df: pl.DataFrame) -> int` — `mart.fx_cross_asset_daily_panel` delete + insert
- `get_panel(pair, start, end) -> list[dict]`

**依存**: Step 1-2

---

### Step 2-2: Cross Asset 特徴量サービス

**ファイル**: `app/services/cross_asset_feature_service.py` (NEW)

#### Part A: instrument-level 特徴量

`rebuild_instrument_features(instrument_id, start=None, end=None) -> int`

入力: `fact.market_bars_daily` → Polars DataFrame
計算（全て Polars で実行）:

| 特徴量 | Polars 式 |
|--------|----------|
| close | `pl.col("close")` |
| ret_1d | `pl.col("close") / pl.col("close").shift(1) - 1` |
| mom_5d | `pl.col("close") / pl.col("close").shift(5) - 1` |
| mom_20d | `pl.col("close") / pl.col("close").shift(20) - 1` |
| rv_5d | `pl.col("ret_1d").rolling_std(5) * (252 ** 0.5)` |
| rv_20d | `pl.col("ret_1d").rolling_std(20) * (252 ** 0.5)` |
| drawdown_20d | `pl.col("close") / pl.col("close").rolling_max(20) - 1` |
| range_pct_1d | `(pl.col("high") - pl.col("low")) / pl.col("close")` |
| weekend_gap_pct | gap_from_prev_close_pct WHERE is_weekend_gap (market_bars_norm から別途取得) |

出力: wide → melt で long-form 化 → `cross_asset_repo.upsert_features()` で保存

#### Part B: pair-level 特徴量

`rebuild_pair_features(pairs, source_instrument_id, start=None, end=None) -> int`

入力:
- USATECH daily: `fact.market_bars_daily` (ret_1d)
- FX spot: `fact_market_series_normalized` (既存テーブル) → ret_1d 計算

ペアごとの処理:
1. USATECH ret_1d と FX ret_1d を obs_date で inner join
2. 計算:

| 特徴量 | 計算 |
|--------|------|
| corr_usatech_pair_20d | `pl.rolling_corr(usatech_ret, pair_ret, window=20)` |
| beta_usatech_pair_60d | `rolling_cov(60) / rolling_var(usatech, 60)` |
| lagcorr_usatech_pair_l1_20d | `rolling_corr(usatech_ret.shift(1), pair_ret, 20)` |
| lagcorr_usatech_pair_l2_20d | `rolling_corr(usatech_ret.shift(2), pair_ret, 20)` |
| risk_filter_flag | ルールベース（§ No-Trade Logic） |

#### No-Trade ルール（`_compute_risk_filter`）:

```python
conditions = [
    df["drawdown_20d"] <= -0.08,
    df["rv_20d"] >= df["rv_20d"].quantile(0.80),
    df["vix_close"] >= df["vix_close"].quantile(0.80),
    df["usd_broad_z"] >= 1.0,
]
hit_count = sum(c.cast(pl.Int32) for c in conditions)
flag = pl.when(hit_count >= 2).then(pl.lit("avoid")).otherwise(pl.lit("ok"))
```

**依存**: Step 1-8, 2-1

---

### Step 2-3: Join Panel サービス

**ファイル**: `app/services/join_panel_service.py` (NEW)

`build_panel(pair, start=None, end=None) -> int`

結合ロジック（obs_date で LEFT JOIN、FX spot が基準）:

```
FX spot close (fact_market_series_normalized)
  LEFT JOIN USATECH features (fact.cross_asset_feature_daily, scope=instrument)
  LEFT JOIN VIX close (fact_market_series_normalized, series_id=VIXCLS)
  LEFT JOIN USD Broad (fact_market_series_normalized, series_id=DTWEXBGS)
  LEFT JOIN rate_spread_3m (fact_derived_factors)
  LEFT JOIN yield_spread_10y (fact_derived_factors)
  LEFT JOIN risk_filter_flag (fact.cross_asset_feature_daily, scope=pair)
  LEFT JOIN regime_label (fact_derived_factors)
```

USATECH features の pivot:
```sql
SELECT obs_date,
       MAX(CASE WHEN feature_name='close' THEN feature_value END) AS usatech_close,
       MAX(CASE WHEN feature_name='ret_1d' THEN feature_value END) AS usatech_ret_1d,
       ...
FROM fact.cross_asset_feature_daily
WHERE feature_scope='instrument' AND scope_id=?
GROUP BY obs_date
```

保存: `cross_asset_repo.upsert_panel()` → `mart.fx_cross_asset_daily_panel`

FX pair → FRED series_id マッピング（既存 `FX_SERIES_MAP` を参照）:
- USDJPY → DEXJPUS
- EURUSD → DEXUSEU
- AUDUSD → DEXUSAL

**依存**: Step 2-1, 2-2

---

### Step 2-4: Cross Asset ルーター

**ファイル**: `app/api/routers/cross_asset.py` (NEW)

エンドポイント:
- `POST /cross-asset/features/rebuild` (EP5)
  - Body: `CrossAssetFeatureBuildRequest`
  - `cross_asset_feature_service.rebuild_instrument_features()` 呼び出し
  - `cross_asset_feature_service.rebuild_pair_features()` 呼び出し
  - Response: `ApiResponse(data={"instrument_features": N, "pair_features": M})`
- `GET /cross-asset/features` (EP6)
  - Query: feature_scope, scope_id, feature_names (comma-separated), start, end
  - `cross_asset_repo.get_features()` で取得
- `GET /panels/fx-crossasset` (EP7)
  - Query: pair, start, end, include_usatech (bool), include_fred (bool)
  - `cross_asset_repo.get_panel()` で取得
  - include_usatech=false の場合は USATECH 列を除外
  - include_fred=false の場合は FRED 列（vix, usd_broad, spreads）を除外

**ファイル**: `app/api/main.py` (MODIFY)
- `from app.api.routers import cross_asset` 追加
- `app.include_router(cross_asset.router, prefix="/api/v1", tags=["cross-asset"])` 追加

**検証**: curl で EP5, EP6, EP7 を手動テスト

**依存**: Step 2-2, 2-3

---

### Step 2-5: Phase 2 テスト

**ファイル**: `tests/unit/test_cross_asset_features.py` (NEW)

テストケース:
- `test_ret_1d` — 既知の close 値で ret_1d を手計算比較
- `test_mom_5d` — 5 日モメンタム検証
- `test_rv_20d` — 20 日 realized vol 検証（annualize 含む）
- `test_drawdown_20d` — ドローダウン計算検証
- `test_corr_20d` — ローリング相関検証（numpy.corrcoef と比較）
- `test_beta_60d` — ローリング β 検証
- `test_lagcorr` — ラグ相関検証
- `test_risk_filter_flag` — 2 条件以上で "avoid"、1 条件以下で "ok"
- `test_empty_data_graceful` — 空データ時のフォールバック

**ファイル**: `tests/integration/test_cross_asset_pipeline.py` (NEW)

テストケース:
- `test_full_pipeline` — in-memory DuckDB で CSV → raw → norm → daily → feature → panel を一気通貫
  - 小さな fixture CSV（50 行程度）を使用
  - 各テーブルの行数を検証
  - パネルのカラム存在を検証

テスト方式: `conftest.py` の in-memory DuckDB fixture を活用

**検証**: `pytest tests/unit/test_cross_asset_features.py tests/integration/test_cross_asset_pipeline.py -v`

**依存**: Step 2-2, 2-3

---

### Step 2-6: 特徴量・パネル再構築ジョブ

**ファイル**: `jobs/rebuild_cross_asset_daily.py` (NEW)

```python
"""
クロスアセット特徴量・パネル再構築
Usage: python jobs/rebuild_cross_asset_daily.py --pairs USDJPY,EURUSD,AUDUSD
注意: API サーバー停止時に実行（DuckDB single-writer）
"""
```

処理:
1. `cross_asset_feature_service.rebuild_instrument_features("usatechidxusd_h4")`
2. `cross_asset_feature_service.rebuild_pair_features(pairs, "usatechidxusd_h4")`
3. `join_panel_service.build_panel(pair)` × 各ペア
4. サマリ出力

**ファイル**: `jobs/rebuild_fx_cross_asset_panel.py` (NEW)

```python
"""
FX クロスアセットパネルのみ再構築（特徴量は再計算しない）
Usage: python jobs/rebuild_fx_cross_asset_panel.py --pairs USDJPY,EURUSD,AUDUSD
"""
```

**依存**: Step 2-2, 2-3

---

### Phase 2 完了チェックポイント

- [ ] `POST /api/v1/cross-asset/features/rebuild` で特徴量が生成される
- [ ] `GET /api/v1/cross-asset/features?feature_scope=instrument&scope_id=usatechidxusd_h4` で 9 特徴量が返る
- [ ] `GET /api/v1/cross-asset/features?feature_scope=pair&scope_id=USDJPY` で 5 特徴量が返る
- [ ] `GET /api/v1/panels/fx-crossasset?pair=USDJPY` でパネルが返る
- [ ] パネルに usatech_close, vix_close, rate_spread_3m 等が含まれる
- [ ] `pytest tests/unit/test_cross_asset_features.py tests/integration/test_cross_asset_pipeline.py -v` 全 pass

---

## Phase 3: Streamlit UI

**ゴール**: 4 つの新画面を追加し、クロスアセット分析を UI で完結させる。

### 共通方針

- 全データ取得は `requests.get/post` で API 経由（DuckDB 直接アクセス禁止: C1）
- API_BASE = `http://localhost:8000/api/v1`
- `st.cache_data(ttl=300)` で API レスポンスをキャッシュ
- 日時表示は `tz_utils.to_jst()` で JST 変換
- Plotly でチャート描画（既存ページと統一）

---

### Step 3-1: Data Upload 画面

**ファイル**: `ui/pages/07_Data_Upload.py` (NEW)

UI 構成:
1. **ファイルアップロード**: `st.file_uploader("CSV/TSV アップロード", type=["csv","tsv","txt"])`
2. **Parser 設定**: vendor_symbol, timeframe, delimiter 等のフォーム
   - デフォルト: USATECHIDXUSD / 240 / タブ / ヘッダーなし
3. **プレビュー**: アップロード後に先頭 20 行を pandas で表示
4. **実行ボタン**: `POST /api/v1/market/uploads` へ multipart 送信
5. **ステータス表示**: upload_id で `GET /api/v1/market/uploads/{id}` をポーリング
   - `st.status()` コンテナで進捗表示
   - accepted → processing → loaded/failed
6. **最近のアップロード一覧**: `upload_repo.list_recent()` 相当の API（EP2 のリスト版追加 or list_recent を EP2 の拡張として提供）

注意: ファイル送信は `requests.post(files=..., data=...)` パターン

**依存**: Step 1-9

---

### Step 3-2: Cross Asset Explorer 画面

**ファイル**: `ui/pages/08_Cross_Asset_Explorer.py` (NEW)

UI 構成:
1. **サイドバー**:
   - Pair selector: `st.selectbox("Pair", ["USDJPY","EURUSD","AUDUSD"])`
   - Date range: `st.date_input` × 2（デフォルト: 過去 3 年）
   - Overlay toggles: `st.multiselect("Overlay", ["USATECH","VIX","USD Broad","3M Spread","10Y Spread"])`
   - Normalization: `st.radio("正規化", ["raw","indexed=100","z-score"])`

2. **メインチャート** (Plotly subplots, 3 行):
   - Row 1: FX pair close (左 y 軸) + USATECH indexed (右 y 軸)
   - Row 2: USATECH ret_1d + drawdown_20d
   - Row 3: USATECH rv_5d + rv_20d

3. **パネルテーブル**: `st.dataframe` で横持ちパネルを表示

データ取得: `GET /api/v1/panels/fx-crossasset?pair={pair}&start={start}&end={end}`

正規化ロジック（pandas で UI 側計算）:
- indexed=100: `series / series.iloc[0] * 100`
- z-score: `(series - series.rolling(60).mean()) / series.rolling(60).std()`

**依存**: Step 2-4

---

### Step 3-3: Lag Correlation 画面

**ファイル**: `ui/pages/09_Lag_Correlation.py` (NEW)

UI 構成:
1. **サイドバー**:
   - Pair selector
   - Lag range: `st.slider("Lag", -5, 5, (-3, 3))`
   - Rolling window: `st.selectbox("Window", [20, 60, 120])`
   - Return basis: `st.radio("Return", ["1d","4h"])`

2. **チャート** (Plotly, 4 パネル):
   - **Lag-Correlation Bar Chart**: 各ラグ (-5 ~ +5) の相関係数を棒グラフ
   - **Rolling Correlation**: 時系列折れ線
   - **Rolling Beta**: 時系列折れ線
   - **Scatter Plot**: USATECH ret vs FX ret + 回帰直線

データ取得:
- パネルデータ: `GET /api/v1/panels/fx-crossasset`
- 特徴量: `GET /api/v1/cross-asset/features`
- ラグ相関の計算は UI 側で pandas `shift()` + `rolling().corr()` で実行（API には保存済み 20d のみ。任意 window は UI で算出）

Scatter + 回帰直線:
```python
from numpy.polynomial.polynomial import polyfit
b, m = polyfit(x, y, 1)
fig.add_trace(go.Scatter(x=x_range, y=m*x_range+b, mode="lines", name="Regression"))
```

**依存**: Step 2-4

---

### Step 3-4: Filter Lab 画面

**ファイル**: `ui/pages/10_Filter_Lab.py` (NEW)

UI 構成:
1. **サイドバー**:
   - Pair selector
   - Date range
   - Threshold sliders:
     - `usatech_drawdown_20d`: `st.slider` (-15% ~ 0%, default=-8%)
     - `usatech_rv_20d percentile`: `st.slider` (50 ~ 99, default=80)
     - `VIX percentile`: `st.slider` (50 ~ 99, default=80)
     - `USD Broad z-score`: `st.slider` (-1.0 ~ 3.0, default=1.0)
   - 条件合成: `st.radio("合成ルール", ["2つ以上","3つ以上","すべて"])`

2. **メインエリア**:
   - **KPI メトリクス** (st.columns × 4):
     - 総日数 / フィルター該当日数 / 除外率 / 該当期間中の平均リターン
   - **タイムライン**: FX close に avoid 期間を赤シェーディング
   - **該当日一覧**: `st.dataframe` で日付 + 各条件の値を表示
   - **ルールエクスポート**: `st.download_button("ルール JSON", json.dumps(rule_dict))`

データ取得: `GET /api/v1/panels/fx-crossasset` からパネル取得し、フィルタリングは UI 側で pandas 処理

フィルターロジック（pandas）:
```python
conditions = pd.DataFrame({
    "dd": df["usatech_drawdown_20d"] <= dd_threshold,
    "rv": df["usatech_rv_20d"] >= df["usatech_rv_20d"].quantile(rv_pct/100),
    "vix": df["vix_close"] >= df["vix_close"].quantile(vix_pct/100),
    "usd": usd_z >= usd_z_threshold,
})
hit_count = conditions.sum(axis=1)
df["avoid"] = hit_count >= min_conditions
```

**依存**: Step 2-4

---

### Phase 3 完了チェックポイント

- [ ] 07_Data_Upload: CSV アップロード → ステータス確認が UI で完結
- [ ] 08_Cross_Asset_Explorer: FX + USATECH オーバーレイ表示、正規化切替
- [ ] 09_Lag_Correlation: ラグ相関棒グラフ、ローリング相関/β、散布図
- [ ] 10_Filter_Lab: スライダーで閾値調整 → 除外日のリアルタイム更新
- [ ] 全画面で DuckDB 直接アクセスなし（API 経由のみ）

---

## Phase 4: 品質＆運用

**ゴール**: 運用安定性の向上とドキュメント整備。

### Step 4-1: Data Quality 画面に USATECH 追加

**ファイル**: `ui/pages/05_Data_Quality.py` (MODIFY)

追加内容:
- Market Bars セクション追加
- `GET /api/v1/quality/market-report?instrument_id=usatechidxusd_h4` で品質レポート表示
- gap / 欠損 / 重複の一覧テーブル
- raw / norm / daily の行数メトリクス

**依存**: Step 1-10

---

### Step 4-2: Parquet エクスポート

**ファイル**: `app/services/market_bar_service.py` (MODIFY)

追加メソッド:
- `export_to_parquet(instrument_id, output_dir, partition_by=["instrument_id","timeframe","bar_year"])`

DuckDB COPY 文:
```sql
COPY (
    SELECT *, year(ts_utc) AS part_year
    FROM fact.market_bars_norm
    WHERE instrument_id = ?
)
TO ? (FORMAT parquet, PARTITION_BY (instrument_id, timeframe, part_year))
```

**依存**: Step 1-8

---

### Step 4-3: ドキュメント更新

**ファイル**: `docs/operation_manual.md` (MODIFY)

追加セクション:
- USATECH データ投入手順
- クロスアセット特徴量の説明
- パネル再構築手順
- 新 UI 画面の使い方

**ファイル**: `docs/improvement_plan.md` (MODIFY)

- クロスアセット統合の完了ステータス更新

**依存**: Phase 1-3 完了後

---

### Phase 4 完了チェックポイント

- [ ] Data Quality 画面に USATECH の品質情報が表示される
- [ ] Parquet エクスポートが動作する
- [ ] 運用ドキュメントが最新化されている

---

## 実行順序サマリ

```
Phase 1 (スキーマ＆取り込み)
  1-1  DDL ファイル
  1-2  DuckDB 初期化拡張          ← 1-1
  1-3  Pydantic モデル
  1-4  バリデーションサービス
  1-5  Upload リポジトリ           ← 1-2
  1-6  Market Bar リポジトリ       ← 1-2
  1-7  CSV 取り込みサービス         ← 1-4, 1-5, 1-6
  1-8  Market Bar サービス         ← 1-6
  1-9  Market Uploads ルーター     ← 1-3, 1-5, 1-7
  1-10 Market Bars ルーター        ← 1-8
  1-11 Phase 1 テスト              ← 1-4, 1-7
  1-12 初期データ投入              ← 1-7, 1-8

Phase 2 (特徴量＆パネル)
  2-1  Cross Asset リポジトリ      ← 1-2
  2-2  特徴量サービス              ← 1-8, 2-1
  2-3  Join Panel サービス         ← 2-1, 2-2
  2-4  Cross Asset ルーター        ← 2-2, 2-3
  2-5  Phase 2 テスト              ← 2-2, 2-3
  2-6  再構築ジョブ                ← 2-2, 2-3

Phase 3 (Streamlit UI)
  3-1  07_Data_Upload              ← 1-9
  3-2  08_Cross_Asset_Explorer     ← 2-4
  3-3  09_Lag_Correlation          ← 2-4
  3-4  10_Filter_Lab               ← 2-4

Phase 4 (品質＆運用)
  4-1  Data Quality 拡張           ← 1-10
  4-2  Parquet エクスポート         ← 1-8
  4-3  ドキュメント                ← Phase 1-3
```

## 並列実行可能なグループ

同一 Phase 内で依存関係のない Step は並列実行可能:

- **Phase 1 並列グループ A**: 1-1, 1-3, 1-4（依存なし）
- **Phase 1 並列グループ B**: 1-5, 1-6（1-2 完了後）
- **Phase 2 並列グループ**: 2-1 は 1-2 のみに依存するため Phase 1 後半と並行可能
- **Phase 3 並列グループ**: 3-1 は Phase 1 完了後即開始可能。3-2/3-3/3-4 は Phase 2 完了後に並列可能
- **Phase 4**: 各 Step は独立しており並列可能
