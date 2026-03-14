# FRED FX Research API リファレンス

**Base URL**: `http://localhost:8000/api/v1`

全レスポンスは以下のエンベロープ形式で返されます：

```json
{
  "status": "ok",
  "as_of": "2026-03-15T12:00:00",
  "timezone": "UTC",
  "data": { ... },
  "warnings": [],
  "errors": []
}
```

---

## 目次

1. [ヘルスチェック](#1-ヘルスチェック)
2. [FRED データ取得](#2-fred-データ取得)
3. [FX パネル構築](#3-fx-パネル構築)
4. [ファクター計算](#4-ファクター計算)
5. [レジーム分析](#5-レジーム分析)
6. [イベント分析](#6-イベント分析)
7. [クロスアセット分析](#7-クロスアセット分析)
8. [リスクフィルタ](#8-リスクフィルタ)
9. [CSV データ取込](#9-csv-データ取込)
10. [OHLC データ](#10-ohlc-データ)
11. [リリースカレンダー](#11-リリースカレンダー)
12. [研究メモ](#12-研究メモ)
13. [データ品質](#13-データ品質)

---

## 1. ヘルスチェック

### GET /health

システムの稼働状態を確認します。

```bash
curl http://localhost:8000/health
```

---

## 2. FRED データ取得

### GET /api/v1/series/search

FRED 系列をキーワード検索します。

| パラメータ | 型 | 必須 | 説明 |
|---|---|---|---|
| q | string | Yes | 検索キーワード |
| limit | int | No | 最大件数（default: 10） |
| refresh_from_fred | bool | No | FRED API から最新を取得 |

```bash
curl "http://localhost:8000/api/v1/series/search?q=exchange+rate+japan&limit=5"
```

### GET /api/v1/series/{series_id}

系列のメタデータを取得します。

```bash
curl http://localhost:8000/api/v1/series/DEXJPUS
```

### POST /api/v1/observations/fetch

FRED から観測値を取得して DuckDB に保存します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| series_id | string | Yes | - | FRED 系列 ID |
| observation_start | date | No | null | 取得開始日 |
| observation_end | date | No | null | 取得終了日 |
| units | string | No | "lin" | データ単位 |
| frequency | string | No | null | 頻度指定 |
| aggregation_method | string | No | "eop" | 集約方法 |
| store_raw | bool | No | true | raw テーブルに保存 |

```bash
curl -X POST http://localhost:8000/api/v1/observations/fetch \
  -H "Content-Type: application/json" \
  -d '{
    "series_id": "DEXJPUS",
    "observation_start": "2020-01-01"
  }'
```

**レスポンス例**:
```json
{
  "data": {
    "series_id": "DEXJPUS",
    "count": 14395,
    "observations": [{"date": "2020-01-02", "value_raw": "108.68", "value_num": 108.68}],
    "batch_id": "abc123"
  }
}
```

---

## 3. FX パネル構築

### POST /api/v1/panel/build

FX 分析パネル（スポット + マクロ因子）を構築します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | USDJPY / EURUSD / AUDUSD |
| date_start | string | Yes | - | 開始日 (YYYY-MM-DD) |
| date_end | string | Yes | - | 終了日 |
| features | array | No | null | 含める特徴量リスト |
| vintage_mode | bool | No | false | ビンテージモード |
| auto_normalize | bool | No | true | 自動正規化 |

```bash
curl -X POST http://localhost:8000/api/v1/panel/build \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "date_start": "2024-01-01",
    "date_end": "2026-03-15",
    "features": ["spot", "rate_spread_3m", "yield_spread_10y"]
  }'
```

### GET /api/v1/panel/pairs

対応通貨ペアの一覧を返します。

```bash
curl http://localhost:8000/api/v1/panel/pairs
```

---

## 4. ファクター計算

### POST /api/v1/factors/compute

指定ペアのファクター（金利差・REER スプレッド等）を計算・保存します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| date_start | string | Yes | - | 開始日 |
| date_end | string | Yes | - | 終了日 |
| features | array | No | null | 計算するファクター名 |
| save | bool | No | true | DB に保存 |

```bash
curl -X POST http://localhost:8000/api/v1/factors/compute \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "date_start": "2024-01-01",
    "date_end": "2026-03-15"
  }'
```

### GET /api/v1/factors/load

保存済みファクターを取得します。

| パラメータ | 型 | 必須 | 説明 |
|---|---|---|---|
| pair | string | Yes | 通貨ペア |
| date_start | date | No | 開始日 |
| date_end | date | No | 終了日 |

```bash
curl "http://localhost:8000/api/v1/factors/load?pair=USDJPY&date_start=2025-01-01"
```

### GET /api/v1/factors/ohlc-derived

OHLC ベースのファクター（Parkinson Vol / Daily Range / Overnight Gap）を取得します。

| パラメータ | 型 | 必須 | 説明 |
|---|---|---|---|
| pair | string | Yes | 通貨ペア |
| start | date | No | 開始日 |
| end | date | No | 終了日 |

```bash
curl "http://localhost:8000/api/v1/factors/ohlc-derived?pair=USDJPY&start=2025-01-01&end=2026-03-15"
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "factor_names": ["parkinson_vol_20d", "daily_range_pct", "daily_range_ma20", "overnight_gap"],
    "row_count": 375,
    "factors": [
      {"obs_date": "2025-01-02", "parkinson_vol_20d": 0.085, "daily_range_pct": 1.23, "daily_range_ma20": 1.15, "overnight_gap": 0.001}
    ]
  }
}
```

---

## 5. レジーム分析

### POST /api/v1/regimes/tag

risk_on / risk_off / carry 状態のレジームタグを計算します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| date_start | string | Yes | - | 開始日 |
| date_end | string | Yes | - | 終了日 |
| save | bool | No | true | 結果を保存 |
| include_stats | bool | No | true | 統計サマリーを含める |

```bash
curl -X POST http://localhost:8000/api/v1/regimes/tag \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "date_start": "2024-01-01",
    "date_end": "2026-03-15"
  }'
```

---

## 6. イベント分析

### POST /api/v1/events/analyze-window

イベント日周辺の FX リターンを分析します。イベントアライン累積リターン・pre/post 統計・hit ratio を返します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| event_dates | array[date] | Yes | - | イベント日のリスト |
| window | int | No | 5 | イベントウィンドウ（営業日） |
| start | date | No | null | 分析期間の開始日 |
| end | date | No | null | 分析期間の終了日 |

```bash
curl -X POST http://localhost:8000/api/v1/events/analyze-window \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "event_dates": ["2025-06-18", "2025-09-17", "2025-12-18", "2026-01-29"],
    "window": 5
  }'
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "window": 5,
    "x_axis": [-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5],
    "events": [
      {"event_date": "2025-06-18", "pre_cum_return": -1.23, "post_cum_return": 0.85, "direction": "UP"}
    ],
    "mean_path": [0.0, -0.1, -0.3, ...],
    "median_path": [0.0, -0.05, -0.2, ...],
    "stats": {
      "valid_event_count": 4,
      "mean_pre_move": -0.93,
      "mean_post_move": 0.84,
      "hit_ratio": 1.0,
      "hit_ratio_label": "4/4"
    }
  }
}
```

---

## 7. クロスアセット分析

### GET /api/v1/panels/fx-crossasset

FX × USATECH クロスアセット日次パネルを取得します。

| パラメータ | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | USDJPY / EURUSD / AUDUSD |
| start | date | No | null | 開始日 |
| end | date | No | null | 終了日 |
| include_usatech | bool | No | true | USATECH 列を含む |
| include_fred | bool | No | true | VIX / USD Broad 列を含む |
| rebuild | bool | No | false | パネルを再構築してから返す |

```bash
curl "http://localhost:8000/api/v1/panels/fx-crossasset?pair=USDJPY&start=2025-01-01&end=2026-03-15"
```

**レスポンスに含まれる列**:
`obs_date`, `pair_close`, `pair_ret_1d`, `usatech_close`, `usatech_ret_1d`, `usatech_mom_5d`, `usatech_mom_20d`, `usatech_rv_5d`, `usatech_rv_20d`, `usatech_drawdown_20d`, `usatech_range_pct_1d`, `vix_close`, `usd_broad_close`, `rate_spread_3m`, `yield_spread_10y`, `event_risk_flag`, `regime_label`

### POST /api/v1/panels/fx-crossasset/rebuild

パネルを全再構築します（正規化 → H4 日次集約 → 特徴量 → パネル結合）。

| パラメータ | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| start | date | No | null | 開始日 |
| end | date | No | null | 終了日 |
| full | bool | No | true | フルパイプライン実行 |

```bash
curl -X POST "http://localhost:8000/api/v1/panels/fx-crossasset/rebuild?pair=USDJPY&full=true"
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "rows_built": 13829,
    "daily_rows": 3319,
    "instrument_features": 29871,
    "pair_features": 15925,
    "normalized_rows": 14493
  }
}
```

### GET /api/v1/cross-asset/features

クロスアセット特徴量を取得します（long-form / wide-form）。

| パラメータ | 型 | 必須 | 説明 |
|---|---|---|---|
| feature_scope | string | Yes | "instrument" / "pair" |
| scope_id | string | Yes | "usatechidxusd_h4" / "USDJPY" 等 |
| feature_names | string | No | カンマ区切り特徴量名 |
| start | date | No | 開始日 |
| end | date | No | 終了日 |
| pivot | bool | No | true で wide-form |

```bash
curl "http://localhost:8000/api/v1/cross-asset/features?feature_scope=instrument&scope_id=usatechidxusd_h4&feature_names=close,rv_20d&pivot=true"
```

### POST /api/v1/cross-asset/features/rebuild

クロスアセット特徴量を再計算します。

```bash
curl -X POST http://localhost:8000/api/v1/cross-asset/features/rebuild \
  -H "Content-Type: application/json" \
  -d '{
    "source_instrument_id": "usatechidxusd_h4",
    "pairs": ["USDJPY", "EURUSD", "AUDUSD"]
  }'
```

### POST /api/v1/cross-asset/rolling-stats

FX × USATECH のローリング相関・β・ラグ相関を計算します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| start | date | Yes | - | 開始日 |
| end | date | Yes | - | 終了日 |
| corr_window | int | No | 20 | 相関のローリング窓 |
| beta_window | int | No | 60 | βのローリング窓 |

```bash
curl -X POST http://localhost:8000/api/v1/cross-asset/rolling-stats \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "start": "2025-01-01",
    "end": "2026-03-15",
    "corr_window": 20,
    "beta_window": 60
  }'
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "corr_window": 20,
    "beta_window": 60,
    "time_series": [
      {"obs_date": "2025-02-05", "rolling_corr": 0.35, "rolling_beta": 0.12}
    ],
    "lag_correlations": {"lag_0": 0.28, "lag_1": 0.15, "lag_2": 0.08},
    "row_count": 294
  }
}
```

---

## 8. リスクフィルタ

### POST /api/v1/filters/evaluate-custom

カスタム閾値でリスクフィルタを評価します。4 条件（DD / RV / VIX / USD-Z）のうち `min_conditions` 以上が発火した日を "avoid" とします。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| pair | string | Yes | - | 通貨ペア |
| start | date | Yes | - | 開始日 |
| end | date | Yes | - | 終了日 |
| thresholds.drawdown_pct | float | No | -8.0 | ドローダウン閾値 (%) |
| thresholds.rv_percentile | int | No | 80 | RV パーセンタイル |
| thresholds.vix_percentile | int | No | 80 | VIX パーセンタイル |
| thresholds.usd_z | float | No | 1.0 | USD Broad Z スコア閾値 |
| thresholds.min_conditions | int | No | 2 | 最小発火条件数 |

```bash
curl -X POST http://localhost:8000/api/v1/filters/evaluate-custom \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "start": "2025-01-01",
    "end": "2026-03-15",
    "thresholds": {
      "drawdown_pct": -5.0,
      "rv_percentile": 75,
      "min_conditions": 2
    }
  }'
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "row_count": 294,
    "avoid_count": 18,
    "avoid_pct": 6.12,
    "records": [
      {"obs_date": "2025-01-02", "custom_flag": "ok", "conditions_met": 0, "cond_dd": false, "cond_rv": false, "cond_vix": false, "cond_usd": false}
    ],
    "thresholds_used": {
      "drawdown_pct": -5.0,
      "rv_percentile": 75,
      "rv_threshold_value": 0.142,
      "vix_percentile": 80,
      "vix_threshold_value": 27.3,
      "usd_z": 1.0,
      "min_conditions": 2
    }
  }
}
```

### POST /api/v1/filters/simulate-pnl

リスクフィルタ適用時の PnL シミュレーションを行います。

```bash
curl -X POST http://localhost:8000/api/v1/filters/simulate-pnl \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "start": "2025-01-01",
    "end": "2026-03-15"
  }'
```

**レスポンス例**:
```json
{
  "data": {
    "pair": "USDJPY",
    "stats": {
      "base_final_return_pct": 0.73,
      "filtered_final_return_pct": 4.19,
      "improvement_pct": 3.46,
      "avoid_days": 18,
      "total_days": 294,
      "avoid_pct": 6.12
    },
    "pnl_series": [
      {"obs_date": "2025-01-02", "base_cumulative": 0.12, "filtered_cumulative": 0.12, "is_avoid": false}
    ]
  }
}
```

---

## 9. CSV データ取込

### POST /api/v1/market/uploads (multipart)

ブラウザ / UI からの CSV ファイルアップロードに使用します。

```bash
curl -X POST http://localhost:8000/api/v1/market/uploads \
  -F "file=@USATECHIDXUSD_H4.csv" \
  -F "vendor_symbol=USATECHIDXUSD" \
  -F "canonical_symbol=USATECHIDXUSD" \
  -F "timeframe=240" \
  -F "delimiter=	" \
  -F "ts_format=%Y-%m-%d %H:%M"
```

### POST /api/v1/market/uploads/from-path (JSON)

AI エージェント向け。ローカルファイルパスを JSON で指定して取り込みます。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| file_path | string | Yes | - | ローカルファイルの絶対パス |
| vendor_symbol | string | No | "USATECHIDXUSD" | ベンダーシンボル |
| canonical_symbol | string | No | "USATECHIDXUSD" | 正規化シンボル |
| instrument_name | string | No | null | 表示名 |
| asset_class | string | No | "equity_index" | アセットクラス |
| timeframe | string | No | "240" | タイムフレーム |
| delimiter | string | No | "\t" | 区切り文字 |
| has_header | bool | No | false | ヘッダー行の有無 |
| ts_format | string | No | "%Y-%m-%d %H:%M" | タイムスタンプ形式 |

```bash
curl -X POST http://localhost:8000/api/v1/market/uploads/from-path \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "/data/USATECHIDXUSD_H4.csv",
    "vendor_symbol": "USATECHIDXUSD",
    "canonical_symbol": "USATECHIDXUSD",
    "timeframe": "240"
  }'
```

**レスポンス (202 Accepted)**:
```json
{
  "data": {
    "upload_id": "upl_20260315_120000_abc12345",
    "instrument_id": "usatechidxusd_h4",
    "status": "accepted",
    "next": "/api/v1/market/uploads/upl_20260315_120000_abc12345"
  }
}
```

### GET /api/v1/market/uploads/{upload_id}

アップロードの処理状態を確認します。

```bash
curl http://localhost:8000/api/v1/market/uploads/upl_20260315_120000_abc12345
```

**レスポンス例**:
```json
{
  "data": {
    "upload_id": "upl_20260315_120000_abc12345",
    "status": "loaded",
    "row_count_detected": 15000,
    "row_count_loaded": 14985,
    "row_count_rejected": 15
  }
}
```

### GET /api/v1/market/uploads

最近のアップロード一覧を返します。

```bash
curl "http://localhost:8000/api/v1/market/uploads?limit=20"
```

### GET /api/v1/market/bars

正規化済みマーケットバーを取得します。

| パラメータ | 型 | 必須 | 説明 |
|---|---|---|---|
| instrument_id | string | Yes | "usatechidxusd_h4" 等 |
| timeframe | string | No | "240" 等 |
| start | date | No | 開始日 |
| end | date | No | 終了日 |
| limit | int | No | 最大行数 |

```bash
curl "http://localhost:8000/api/v1/market/bars?instrument_id=usatechidxusd_h4&start=2025-01-01&limit=100"
```

### POST /api/v1/market/bars/rebuild-daily

H4 バーから日次バーを再集約します。

```bash
curl -X POST http://localhost:8000/api/v1/market/bars/rebuild-daily \
  -H "Content-Type: application/json" \
  -d '{"instrument_id": "usatechidxusd_h4"}'
```

---

## 10. OHLC データ

### GET /api/v1/ohlc/daily/{pair}

CSV ベースの日足 OHLC を取得します。

```bash
curl "http://localhost:8000/api/v1/ohlc/daily/USDJPY?start_date=2025-01-01&limit=100"
```

### GET /api/v1/ohlc/intraday/{pair}/{timeframe}

イントラデイ OHLC を取得します。

```bash
curl "http://localhost:8000/api/v1/ohlc/intraday/USDJPY/60?limit=50"
```

### GET /api/v1/ohlc/summary

全ペア・全タイムフレームのデータサマリーを返します。

```bash
curl http://localhost:8000/api/v1/ohlc/summary
```

---

## 11. リリースカレンダー

### POST /api/v1/releases/fetch

FRED から Release Calendar を取得・保存します。

| フィールド | 型 | 必須 | デフォルト | 説明 |
|---|---|---|---|---|
| start_date | date | No | null | 開始日 |
| end_date | date | No | null | 終了日 |

```bash
curl -X POST http://localhost:8000/api/v1/releases/fetch \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2026-03-15"}'
```

### GET /api/v1/releases/list

保存済みリリースカレンダーを一覧取得します。

```bash
curl "http://localhost:8000/api/v1/releases/list?start_date=2025-01-01&limit=100"
```

---

## 12. 研究メモ

### POST /api/v1/memo/generate

研究メモを生成・保存します。

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| pair | string | Yes | 通貨ペア |
| date_start | string | Yes | 開始日 |
| date_end | string | Yes | 終了日 |
| research_question | string | Yes | 研究課題 |
| hypothesis | string | Yes | 仮説 |
| observations | string | Yes | 観測事実 |
| uncertainty | string | No | 不確実性 |
| next_experiment | string | No | 次の実験 |
| risk_controls | string | No | リスク管理 |
| selected_factors | array | No | 関連ファクター名 |

```bash
curl -X POST http://localhost:8000/api/v1/memo/generate \
  -H "Content-Type: application/json" \
  -d '{
    "pair": "USDJPY",
    "date_start": "2025-01-01",
    "date_end": "2026-03-15",
    "research_question": "日米金利差拡大時の USDJPY の反応",
    "hypothesis": "金利差拡大は円安に寄与する",
    "observations": "2025年Q1に金利差が150bp超に拡大"
  }'
```

### GET /api/v1/memo/list

保存済みメモの一覧を返します。

```bash
curl http://localhost:8000/api/v1/memo/list
```

### GET /api/v1/memo/{memo_id}

特定のメモを取得します。

```bash
curl http://localhost:8000/api/v1/memo/memo_20260315_abc123
```

### GET /api/v1/memo/{memo_id}/markdown

メモを Markdown 形式で取得します。

```bash
curl http://localhost:8000/api/v1/memo/memo_20260315_abc123/markdown
```

---

## 13. データ品質

### POST /api/v1/data-quality/audit

データ鮮度監査を実行します。

```bash
curl -X POST http://localhost:8000/api/v1/data-quality/audit
```

### GET /api/v1/data-quality/ingestion-log

取り込みログを取得します。

```bash
curl "http://localhost:8000/api/v1/data-quality/ingestion-log?limit=20"
```

### GET /api/v1/data-quality/registry

系列レジストリを取得します。

```bash
curl http://localhost:8000/api/v1/data-quality/registry
```

### GET /api/v1/quality/market-report

マーケットバーの品質レポートを取得します。

```bash
curl "http://localhost:8000/api/v1/quality/market-report?instrument_id=usatechidxusd_h4"
```

---

## AI エージェント向けワークフロー例

### 1. 初期データセットアップ

```bash
# FX スポットデータを取得
curl -X POST $API/observations/fetch -H "Content-Type: application/json" \
  -d '{"series_id": "DEXJPUS"}'
curl -X POST $API/observations/fetch -H "Content-Type: application/json" \
  -d '{"series_id": "DEXUSEU"}'
curl -X POST $API/observations/fetch -H "Content-Type: application/json" \
  -d '{"series_id": "DEXUSAL"}'

# VIX / USD Broad
curl -X POST $API/observations/fetch -H "Content-Type: application/json" \
  -d '{"series_id": "VIXCLS"}'
curl -X POST $API/observations/fetch -H "Content-Type: application/json" \
  -d '{"series_id": "DTWEXBGS"}'

# USATECH CSV 取込
curl -X POST $API/market/uploads/from-path -H "Content-Type: application/json" \
  -d '{"file_path": "/path/to/USATECHIDXUSD_H4.csv"}'

# パネル再構築（全ペア）
curl -X POST "$API/panels/fx-crossasset/rebuild?pair=USDJPY&full=true"
curl -X POST "$API/panels/fx-crossasset/rebuild?pair=EURUSD&full=true"
curl -X POST "$API/panels/fx-crossasset/rebuild?pair=AUDUSD&full=true"
```

### 2. 分析ワークフロー

```bash
# パネルデータ取得
curl "$API/panels/fx-crossasset?pair=USDJPY&start=2025-01-01"

# ローリング相関分析
curl -X POST $API/cross-asset/rolling-stats -H "Content-Type: application/json" \
  -d '{"pair": "USDJPY", "start": "2025-01-01", "end": "2026-03-15"}'

# FOMC イベント分析
curl -X POST $API/events/analyze-window -H "Content-Type: application/json" \
  -d '{"pair": "USDJPY", "event_dates": ["2025-06-18", "2025-09-17", "2025-12-18"], "window": 5}'

# リスクフィルタ評価
curl -X POST $API/filters/evaluate-custom -H "Content-Type: application/json" \
  -d '{"pair": "USDJPY", "start": "2025-01-01", "end": "2026-03-15"}'

# PnL シミュレーション
curl -X POST $API/filters/simulate-pnl -H "Content-Type: application/json" \
  -d '{"pair": "USDJPY", "start": "2025-01-01", "end": "2026-03-15", "thresholds": {"drawdown_pct": -5.0, "min_conditions": 2}}'
```

---

## OpenAPI ドキュメント

対話型 API ドキュメントは以下で利用可能です：

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json
