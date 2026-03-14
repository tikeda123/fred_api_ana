# FRED FX Research Workbench 運用マニュアル

**バージョン**: 1.4
**対象システム**: FRED FX Research Workbench
**作成日**: 2026-03-14
**更新日**: 2026-03-15

| バージョン | 主な変更内容 |
|---|---|
| v1.1 | 政策金利・BIS REER・jpnassets_yoy ファクター追加、スキーマ拡張 |
| v1.2 | CSV OHLC データローダー追加、FRED 欠測補完、OHLC API エンドポイント、UI 改善 |
| v1.3 | Parkinson Vol・Daily Range・Overnight Gap ファクター追加、Pair Workspace に Volatility/Range チャート追加、JST タイムゾーン表示対応 |
| v1.4 | USATECHIDXUSD クロスアセット統合（H4 CSV 取り込み・特徴量計算・パネル構築・4 新 UI 画面・Parquet エクスポート） |

---

## 目次

1. [システム概要](#1-システム概要)
2. [環境構築](#2-環境構築)
3. [起動・停止](#3-起動停止)
4. [画面操作ガイド](#4-画面操作ガイド)
5. [データ取り込み手順](#5-データ取り込み手順)
6. [バッチジョブ](#6-バッチジョブ)
7. [API リファレンス](#7-api-リファレンス)
8. [データ管理](#8-データ管理)
9. [トラブルシューティング](#9-トラブルシューティング)
10. [用語集](#10-用語集)

---

## 1. システム概要

### 1.1 目的

FRED（Federal Reserve Economic Data）API を使って USD/JPY・EUR/USD・AUD/USD を中心とした FX 市場の**マクロ・金利・レジーム・クロスアセット分析**を行うための Research Workbench です。

### 1.2 できること

| 機能 | 説明 |
|---|---|
| データ取得 | FRED から FX・政策金利・市場金利・VIX・BIS REER・商品データを自動取得 |
| CSV OHLC 取込 | ブローカー CSV（1分〜日足）をロードし、FRED 欠測日を close 値で自動補完 |
| 正規化 | 通貨方向・欠測・頻度（source_frequency 自動設定）を統一管理 |
| 因子計算 | 政策金利差・市場金利差・REER スプレッド・カーブ差・BoJ 資産 YoY・モメンタム・ボラティリティ・Parkinson Vol・Daily Range・Overnight Gap |
| クロスアセット分析 | USATECH (US Tech 100) × FX の相関・β・ドローダウン・risk_filter_flag 計算（v1.4）|
| レジーム分析 | risk_on/off・carry状態・カーブ形状を自動タグ付け |
| イベント分析 | FOMC等リリース周辺のFXリターン観測 |
| 研究メモ | 仮説・観測・不確実性を構造化して保存・Markdown出力 |

### 1.3 対象外

- ティックデータ・秒足・板情報（1分足までは CSV ロード対応）
- 注文執行・スリッページ推定
- ブローカー接続

### 1.4 システム構成

```
FRED API                     CSV (OHLC 1min〜日足)          USATECH CSV (H4/D1)
  │                               │                               │
  ▼                               ▼                               ▼
Ingestion Service          CSV Loader Service            load_usatech_csv.py
  │                               │                               │
  ├─ Raw Parquet                  └─ fact_ohlc_intraday           └─ fact.market_bars_raw
  ├─ DuckDB                            │                               │
  │                                    ├─ FRED 欠測補完               ├─ fact.market_bars_norm
  ▼                                    ▼                               └─ fact.market_bars_daily
Normalize / Factor / Regime Engine                                          │
  │                                                                         ▼
  ├─ fact_market_series_normalized                              cross_asset_feature_service
  ├─ fact_derived_factors                                           │
  │                                                                 ├─ fact.cross_asset_feature_daily
  └─────────────────────────────────────────────────────────────────┤
                                                                     ▼
                                                             join_panel_service
                                                                 │
                                                                 └─ mart.fx_cross_asset_daily_panel
                                                                         │
                                                                         ▼
                                                             Analysis API (FastAPI :8000)
                                                                         │
                                                                         ▼
                                                                 Streamlit UI (:8501)
```

---

## 2. 環境構築

### 2.1 前提条件

| 項目 | バージョン |
|---|---|
| Python | 3.11 以上 |
| pip | 最新版推奨 |
| OS | Linux / macOS / Windows (WSL2) |

### 2.2 インストール手順

```bash
# 1. プロジェクトディレクトリに移動
cd fred-fx-research

# 2. 依存パッケージをインストール
pip install fastapi uvicorn httpx pydantic pydantic-settings \
            duckdb polars structlog tenacity python-dotenv \
            streamlit plotly requests
```

### 2.3 環境変数の設定（`.env`）

```env
APP_ENV=dev
APP_HOST=0.0.0.0
APP_PORT=8000
FRED_API_KEY=<あなたのFRED APIキー>   # ← 必須
DUCKDB_PATH=./data/fred_fx.duckdb
RAW_DATA_ROOT=./data/raw
NORMALIZED_DATA_ROOT=./data/normalized
DERIVED_DATA_ROOT=./data/derived
DEFAULT_TZ=UTC
STREAMLIT_SERVER_PORT=8501
```

> **タイムゾーン**: DB・API レスポンスは UTC で保存。Streamlit UI 上の日時表示は自動的に **JST (UTC+9)** に変換されます。

> **FRED API キー**: https://fred.stlouisfed.org/docs/api/api_key.html で無料取得できます。

### 2.4 初回データ取り込み

```bash
# 1. FRED データをバックフィル（初回のみ、5〜10分程度）
python jobs/backfill_series.py --start 2020-01-01

# 2. USATECH CSV をロード（クロスアセット分析を使う場合）
# ※ API サーバー停止中に実行
python jobs/load_usatech_csv.py \
  --data-dir /path/to/data/us_index \
  --timeframes 240

# 3. クロスアセット特徴量・パネルを構築
python jobs/rebuild_cross_asset_daily.py \
  --instrument-id usatechidxusd_h4 \
  --pairs USDJPY,EURUSD,AUDUSD
```

---

## 3. 起動・停止

### 3.1 起動

**ターミナル1（API サーバー）**
```bash
cd fred-fx-research
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload
```

**ターミナル2（UI）**
```bash
cd fred-fx-research
streamlit run ui/Home.py --server.port 8501 --server.address 0.0.0.0
```

**バックグラウンド起動（推奨）**
```bash
nohup uvicorn app.api.main:app --host 0.0.0.0 --port 8000 > /tmp/api.log 2>&1 &
nohup streamlit run ui/Home.py --server.port 8501 --server.address 0.0.0.0 --server.headless true > /tmp/ui.log 2>&1 &
```

### 3.2 アクセス URL

| サービス | URL |
|---|---|
| Streamlit UI | http://localhost:8501 |
| API ドキュメント (Swagger) | http://localhost:8000/docs |
| API ヘルスチェック | http://localhost:8000/health |

### 3.3 停止

```bash
kill $(lsof -t -i:8000)   # API
kill $(lsof -t -i:8501)   # UI
```

### 3.4 SSH ポートフォワード（リモートサーバー接続時）

```bash
ssh -L 8501:localhost:8501 -L 8000:localhost:8000 user@<サーバーIP>
```

---

## 4. 画面操作ガイド

### 4.1 Home（トップページ）

| 表示要素 | 説明 |
|---|---|
| System Status | DuckDB・Parquet・FRED API の接続状態（🟢 ok / 🔴 degraded） |
| 主要ペア最新スポット | USDJPY / EURUSD / AUDUSD の最新値と前日比（%） |
| 現在のレジーム状態 | 各ペアの Risk / Carry / Curve / USD 状態をアイコン表示 |

---

### 4.2 Series Catalog（series 検索）

**サイドバー** → `Series Catalog`

FREDの series を検索し、メタデータを確認する画面です。

**操作手順**

1. `キーワード` 欄に検索語を入力（例: `Japanese Yen`、`Treasury`）
2. `最大件数` を設定（デフォルト: 50）
3. `FRED から再取得` にチェックすると最新の情報を FRED から取得します
4. `検索` ボタンをクリック

| 列名 | 説明 |
|---|---|
| series_id | FRED の系列ID（例: DEXJPUS） |
| title | 系列名 |
| frequency | 頻度（D=日次、M=月次、Q=四半期） |
| observation_end | 最終観測日 |
| domain | データ種別（fx_spot、rate_3m、yield_10y など） |
| freshness_status | 鮮度（ok / warning / reject） |

---

### 4.3 Pair Workspace（ペア分析）

**サイドバー** → `Pair Workspace`

通貨ペアの中核分析画面です。

**チャート構成**

```
[1段] spot レート
[2段] Policy Rate Spread
[3段] Short Rate Spread 3M
[4段] Long Rate Spread 10Y
[5段] VIX / USD Broad
[6段] Volatility: Parkinson Vol 20d vs Realized Vol 20d  （v1.3）
[7段] Range / Gap: Daily Range % + MA20 + Overnight Gap  （v1.3）
```

**OHLC ファクター（v1.3）**

| ファクター名 | 計算式 | 意味 |
|---|---|---|
| `parkinson_vol_20d` | `sqrt(rolling_mean(ln(H/L)², 20) / (4·ln2))` | OHLC ベースの効率的ボラティリティ |
| `daily_range_pct` | `(high - low) / open × 100` | 日中値幅 (%) |
| `daily_range_ma20` | daily_range_pct の 20日移動平均 | 値幅の平滑化 |
| `overnight_gap` | `open_today / close_yesterday - 1` | ギャップリスク |

---

### 4.4 Regime Dashboard（レジーム分析）

**サイドバー** → `Regime Dashboard`

| カード | 表示例 |
|---|---|
| Risk State | 🟢 Risk ON / 🔴 Risk OFF |
| Carry State | 🟢 Carry+ / 🔴 Carry− / ⚪ 中立 |
| US Curve | 🟢 順イールド / 🔴 逆転 / 🟡 フラット |
| USD State | 🔵 USD強 / 🟡 USD弱 / ⚪ 中立 |

| タブ | 判定条件 |
|---|---|
| Risk State | VIX が過去 252 日の 75 パーセンタイル超 → 🔴 risk_off |
| Carry State | 3M 金利差が +0.1% 超 → carry_positive |
| Curve State | US 10Y-3M > 0.5% → normal / < 0% → inverted |
| USD State | USD Broad の 252 日 z-score > 1.0 → usd_strong |

---

### 4.5 Event Study（イベント分析）

**サイドバー** → `Event Study`

FOMC・ECB 等の release イベント前後の FX リターンを分析します。

```
X 軸: イベント日を 0 として前後 N 営業日
Y 軸: 累積リターン (%)
薄い青線: 各イベントの個別パス / 濃い青線: 平均 / オレンジ: 中央値
```

---

### 4.6 Data Quality（データ品質）

**サイドバー** → `Data Quality`

| セクション | 内容 |
|---|---|
| Freshness Audit | 全 series の鮮度判定（ok / warning / reject）|
| Missing Heatmap | 月次欠測率ヒートマップ（ペア選択可）|
| **Market Bars 品質レポート** | USATECH の Raw/Norm/Daily 行数・異常 Range・日付ギャップ（**v1.4**）|
| Ingestion Log | 直近取り込み履歴 |
| Series Registry | 登録済み全 series 一覧 |

**Market Bars 品質レポートの確認手順（v1.4）**

1. 「USATECH データ品質を確認する」エキスパンダーを開く
2. Instrument ID（例: `usatechidxusd_h4`）と期間を設定
3. 「品質レポート取得」をクリック
4. Raw / Norm / Daily の行数と日付ギャップ一覧を確認

---

### 4.7 Data Upload（USATECH CSV アップロード）（v1.4）

**サイドバー** → `Data Upload`

USATECH (US Tech 100 Index) の OHLC CSV ファイルをアップロードし、クロスアセット分析用データを登録します。

**操作手順**

1. サイドバーで `Instrument ID`（例: `usatechidxusd_h4`）・タイムフレーム・区切り文字を設定
2. `CSV ファイルを選択` でブローカー CSV をドラッグ＆ドロップ
3. `アップロード開始` をクリック
4. ステータス確認（`自動更新` トグルで 5 秒ごとにポーリング）
5. `loaded` 確認後、`rebuild_cross_asset_daily.py` で特徴量を再計算

**ステータス遷移**

```
accepted → processing → loaded（成功）
                      → failed（失敗: エラーメッセージを確認）
```

**USATECH CSV ファイル形式**

- TSV（タブ区切り）、ヘッダーなし
- 列順: `datetime\tOpen\tHigh\tLow\tClose\tVolume`
- タイムスタンプ例: `2024.01.02 00:00`
- ファイル名例: `USATECHIDXUSD240.csv`（H4 足）

---

### 4.8 Cross Asset Explorer（クロスアセット分析）（v1.4）

**サイドバー** → `Cross Asset Explorer`

FX ペアと USATECH のクロスアセット分析を行います。

**チャート構成（3段）**

```
[1段] FX Spot（左 y 軸）+ USATECH（右 y 軸）
      ※ VIX overlay / avoid 日を赤シェーディング
[2段] USATECH RV20d（%）vs FX RV20d（%）
[3段] USATECH DD20d（%）バーチャート（-8% ラインで赤表示）
```

| オーバーレイトグル | 説明 |
|---|---|
| USATECH オーバーレイ | USATECH close を右 y 軸に表示 |
| VIX オーバーレイ | VIX を右 y 軸に重ねて表示 |
| Risk Filter フラグ | avoid 日（2 条件以上）を赤でシェーディング |

> **パネルが空の場合**: サイドバーの「パネルを再構築」ボタンをクリックするか、`rebuild_cross_asset_daily.py` を実行してください。

---

### 4.9 Lag Correlation（遅行相関分析）（v1.4）

**サイドバー** → `Lag Correlation`

USATECH リターンと FX ペアリターンの遅行相関・ローリング統計を分析します。

**チャート構成（2×2）**

| パネル | 内容 |
|---|---|
| Rolling Corr | 指定ウィンドウのローリング相関係数（時系列） |
| Rolling β | 指定ウィンドウのローリング β 係数（時系列） |
| Lag Corr Bar | ラグ 0〜N 日の相関係数バーチャート |
| Scatter | USATECH ret vs FX ret 散布図 + OLS 回帰線（Rolling Corr でカラー）|

| スライダー | デフォルト | 説明 |
|---|---|---|
| 相関ウィンドウ | 20日 | ローリング相関の計算窓 |
| β ウィンドウ | 60日 | ローリング β の計算窓 |
| 最大ラグ | 5日 | ラグ相関バーの最大ラグ |

**読み方のポイント**
- ラグ相関が lag=1 で最大 → USATECH が 1 日先行して FX を動かす
- Rolling Corr が 0.3 超の期間 → USATECH が FX のドライバーになっている局面

---

### 4.10 Filter Lab（no-trade フィルター実験）（v1.4）

**サイドバー** → `Filter Lab`

risk_filter_flag のしきい値を動的に変更し、no-trade フィルターの影響をシミュレーションします。

**条件ロジック（2 条件以上成立 → avoid）**

| 条件 | デフォルト | 意味 |
|---|---|---|
| ドローダウン | DD ≤ -8.0% | USATECH DD20d がしきい値以下 |
| RV パーセンタイル | RV > P80 | USATECH RV20d がパーセンタイル超 |
| VIX パーセンタイル | VIX > P80 | VIX がパーセンタイル超 |
| USD Z-score | Z ≥ 1.0 | USD Broad の Z-score がしきい値以上 |
| 最小条件数 | 2 | N 条件以上で avoid |

**チャート構成（3段）**
```
[1段] FX Spot + avoid 日の赤シェーディング + デフォルトフラグ（紫）
[2段] Buy & Hold 累積リターン vs Filter 適用 累積リターン
[3段] 条件フラグ積み上げバー（DD / RV / VIX / USD-Z）
```

**エクスポート**

- `JSON ダウンロード` でフィルター設定・しきい値・avoid 日一覧を JSON で保存
- システムトレードへの組み込みや分析ログとして利用

---

### 4.11 Memo Builder（研究メモ）

**サイドバー** → `Memo Builder`

分析結果を構造化した研究メモとして保存・出力する画面です。

| 入力項目 | 例 |
|---|---|
| Research Question | USDJPY の動きを説明する主要因子は何か |
| Hypothesis | USATECH の急落局面では USDJPY はリスクオフで円高になる |
| Observations | corr_20d が 0.3 超の期間は USATECH が FX の先行指標となっている |
| Uncertainty | 月次金利データのため日次動きを捉えきれない |
| Next Experiment | Filter Lab で avoid 期間を絞りリターン改善を確認 |
| Risk Controls | risk_filter_flag の条件は定期的に市場環境に合わせて見直す |

---

## 5. データ取り込み手順

### 5.1 初回バックフィル（FRED データ）

```bash
# 2020年1月から全 series をバックフィル（初回のみ）
python jobs/backfill_series.py --start 2020-01-01

# 特定 series だけを取り込む場合
python jobs/backfill_series.py --series DEXJPUS VIXCLS --start 2020-01-01
```

**デフォルトで取り込む series**

| カテゴリ | Series ID | 内容 |
|---|---|---|
| FX スポット | DEXJPUS | USD/JPY 日次 |
| FX スポット | DEXUSEU | EUR/USD 日次 |
| FX スポット | DEXUSAL | AUD/USD 日次 |
| USD 指数 | DTWEXBGS | Nominal Broad Dollar Index |
| 3M 金利 | IR3TIB01USM156N | 米国 3M インターバンク |
| 3M 金利 | IR3TIB01JPM156N | 日本 3M インターバンク |
| 3M 金利 | IR3TIB01EZM156N | ユーロ圏 3M |
| 3M 金利 | IR3TIB01AUM156N | 豪州 3M |
| 10Y 利回り | IRLTLT01USM156N | 米国 10Y |
| 10Y 利回り | IRLTLT01JPM156N | 日本 10Y |
| 10Y 利回り | IRLTLT01EZM156N | ユーロ圏 10Y |
| 10Y 利回り | IRLTLT01AUM156N | 豪州 10Y |
| リスク | VIXCLS | VIX 恐怖指数 |
| 金融ストレス | STLFSI4 | St.Louis FSI |
| 商品 | DCOILWTICO | WTI 原油 |
| BoJ | JPNASSETS | 日銀総資産 |
| 政策金利 | EFFR | 米国 FF レート (日次) |
| 政策金利 | ECBDFR | ECB 預金ファシリティ金利 |
| 政策金利 | IRSTCI01JPM156N | 日本 短期金利 (月次) |
| 政策金利 | IRSTCI01AUM156N | 豪州 短期金利 (月次) |
| BIS REER | RBUSBIS | 米国 実質実効為替レート (月次) |
| BIS REER | RBJPBIS | 日本 実質実効為替レート (月次) |
| BIS REER | RBAUBIS | 豪州 実質実効為替レート (月次) |
| BIS REER | RBXMBIS | ユーロ圏 実質実効為替レート (月次) |

---

### 5.2 USATECH CSV の取り込み（v1.4）

USATECH (US Tech 100 Index) の H4 CSV をロードしてクロスアセット分析を有効にします。

> **⚠️ 重要**: DuckDB は single-writer のため、**API サーバーを停止してから**実行してください。

**方法A: バッチジョブ（推奨）**

```bash
# API サーバーを停止
kill $(lsof -t -i:8000)

# USATECH H4 CSV を一括ロード
python jobs/load_usatech_csv.py \
  --data-dir /path/to/data/us_index \
  --timeframes 240

# API サーバーを再起動
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload &
```

**方法B: UI アップロード（小ファイル向け）**

1. Streamlit の `Data Upload` 画面を開く
2. Instrument ID = `usatechidxusd_h4`、タイムフレーム = `h4` を設定
3. CSV ファイルをアップロード → `loaded` まで待機

**USATECH CSV ファイル形式**

| 項目 | 値 |
|---|---|
| 区切り文字 | タブ |
| ヘッダー | なし |
| 列順 | datetime, Open, High, Low, Close, Volume |
| タイムスタンプ形式 | `%Y.%m.%d %H:%M` |
| ファイル配置 | `{data-dir}/USATECHIDXUSD240.csv` |

---

### 5.3 クロスアセット特徴量・パネルの構築（v1.4）

USATECH CSV ロード後に特徴量とパネルを構築します。

```bash
# 特徴量 + パネルをフルビルド
python jobs/rebuild_cross_asset_daily.py \
  --instrument-id usatechidxusd_h4 \
  --pairs USDJPY,EURUSD,AUDUSD

# 特徴量はそのままパネルだけ更新する場合
python jobs/rebuild_fx_cross_asset_panel.py \
  --pairs USDJPY,EURUSD,AUDUSD
```

---

### 5.4 FX ブローカー CSV OHLC のロード（v1.2）

```bash
# 全ペア・全タイムフレームをロード + FRED 欠測補完
make load-csv

# カスタム設定
python jobs/load_csv_ohlc.py \
  --data-dir /path/to/csv/data \
  --pairs USDJPY EURUSD \
  --timeframes 60 240 1440 \
  --no-supplement
```

**FX OHLC CSV ファイル形式**

- TSV（タブ区切り）、ヘッダーなし
- 列順: `datetime_open\tOpen\tHigh\tLow\tClose\tVolume`
- 配置: `{data-dir}/{pair_lower}/{PAIR}_{timeframe}.csv`（例: `data/usdjpy/USDJPY_1440.csv`）

**FRED 欠測補完の仕組み**
- CSV 日足の close 値で FRED に存在しない日付を `fact_market_series_normalized` に挿入
- 補完データには `is_supplemental = TRUE` が付与（バックテスト時は `FALSE` でフィルタ）

---

### 5.5 API から個別取り込み

```bash
# DEXJPUS を 2023年から取り込む
curl -X POST http://localhost:8000/api/v1/observations/fetch \
  -H "Content-Type: application/json" \
  -d '{
    "series_id": "DEXJPUS",
    "observation_start": "2023-01-01",
    "observation_end": "2026-03-15",
    "store_raw": true,
    "normalize": false
  }'
```

---

### 5.6 データ取り込みフロー全体図

```
[FRED ルート]
1. observations/fetch  →  fact_series_observations_raw (DuckDB)
                       →  data/raw/ (Parquet)
2. normalize_series    →  fact_market_series_normalized (DuckDB)
                       →  data/normalized/ (Parquet)

[FX OHLC CSV ルート]
3. load_csv_ohlc       →  fact_ohlc_intraday (DuckDB)
                       →  FRED 欠測補完 → fact_market_series_normalized (is_supplemental=TRUE)

[USATECH ルート]（v1.4）
4. load_usatech_csv    →  fact.market_bars_raw / norm / daily (DuckDB)
5. rebuild_instrument  →  fact.cross_asset_feature_daily (instrument-level)
6. rebuild_pair        →  fact.cross_asset_feature_daily (pair-level + risk_filter_flag)
7. build_panel         →  mart.fx_cross_asset_daily_panel

[分析]
8. panel/build         →  data/derived/factor_group=panel/ (Parquet)
9. factors/compute     →  fact_derived_factors (DuckDB)
10. regimes/tag        →  data/derived/factor_group=regimes/ (Parquet)
```

---

## 6. バッチジョブ

### 6.1 デイリー更新（daily_refresh.py）

```bash
python jobs/daily_refresh.py
```

**処理内容**
1. 日次 FX / VIX / USD Broad を直近 30 日分 refresh
2. 正規化
3. USDJPY / EURUSD / AUDUSD の factor・regime を再計算
4. 全 series の鮮度監査

**cron 設定例（毎営業日 7 時に実行）**
```bash
0 7 * * 1-5 cd /path/to/fred-fx-research && python jobs/daily_refresh.py >> /tmp/daily.log 2>&1
```

---

### 6.2 Release Calendar 更新（refresh_release_calendar.py）

```bash
python jobs/refresh_release_calendar.py
python jobs/refresh_release_calendar.py --start 2026-01-01 --end 2026-12-31
```

---

### 6.3 USATECH 初期ロード（load_usatech_csv.py）（v1.4）

USATECH H4 CSV を DuckDB にロードします（初回のみ実行）。

```bash
# API サーバー停止後に実行
python jobs/load_usatech_csv.py \
  --data-dir /path/to/data/us_index \
  --timeframes 240,1440
```

| 引数 | デフォルト | 説明 |
|---|---|---|
| `--data-dir` | 必須 | CSV ファイルのルートディレクトリ |
| `--timeframes` | `240` | 対象タイムフレーム（分）カンマ区切り |

---

### 6.4 クロスアセット再構築（rebuild_cross_asset_daily.py）（v1.4）

特徴量とパネルを再構築します。

```bash
# フルビルド（instrument 特徴量 + pair 特徴量 + パネル）
python jobs/rebuild_cross_asset_daily.py \
  --instrument-id usatechidxusd_h4 \
  --pairs USDJPY,EURUSD,AUDUSD \
  --start 2020-01-01

# 特徴量のみ（パネルをスキップ）
python jobs/rebuild_cross_asset_daily.py \
  --instrument-id usatechidxusd_h4 \
  --pairs USDJPY,EURUSD,AUDUSD \
  --skip-panel
```

| 引数 | デフォルト | 説明 |
|---|---|---|
| `--instrument-id` | `usatechidxusd_h4` | ソース Instrument ID |
| `--pairs` | `USDJPY,EURUSD,AUDUSD` | 対象 FX ペア（カンマ区切り）|
| `--start` | (全期間) | 再構築開始日（YYYY-MM-DD）|
| `--end` | (全期間) | 再構築終了日（YYYY-MM-DD）|
| `--skip-panel` | false | 特徴量計算のみ実行 |

---

### 6.5 パネルのみ再構築（rebuild_fx_cross_asset_panel.py）（v1.4）

特徴量は変えずパネルだけ更新します。

```bash
python jobs/rebuild_fx_cross_asset_panel.py \
  --pairs USDJPY,EURUSD,AUDUSD
```

---

### 6.6 FX OHLC CSV ロード（load_csv_ohlc.py）（v1.2）

```bash
python jobs/load_csv_ohlc.py --data-dir /path/to/data
python jobs/load_csv_ohlc.py --data-dir /path/to/data --timeframes 1440
python jobs/load_csv_ohlc.py --data-dir /path/to/data --pairs USDJPY EURUSD
python jobs/load_csv_ohlc.py --data-dir /path/to/data --no-supplement
```

| 引数 | デフォルト | 説明 |
|---|---|---|
| `--data-dir` | 必須 | CSV ファイルのルートディレクトリ |
| `--pairs` | USDJPY EURUSD AUDUSD | 対象ペア |
| `--timeframes` | 1 5 15 30 60 240 1440 | 対象タイムフレーム (分) |
| `--no-supplement` | (無効) | FRED 欠測補完をスキップ |

---

### 6.7 ジョブ実行状況の確認

```bash
# API ログ
tail -f /tmp/api.log

# UI ログ
tail -f /tmp/ui.log

# アップロード監査ログ（クロスアセット）
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
rows = conn.execute(
    'SELECT upload_id, instrument_id, status, row_count_loaded, finished_at FROM ops.fact_upload_audit ORDER BY started_at DESC LIMIT 5'
).fetchall()
for r in rows: print(r)
"

# FRED 取り込み監査ログ
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
rows = conn.execute(
    'SELECT job_name, status, record_count, started_at FROM audit_ingestion_runs ORDER BY started_at DESC LIMIT 10'
).fetchall()
for r in rows: print(r)
"
```

---

## 7. API リファレンス

Swagger UI: http://localhost:8000/docs

### 7.1 主要エンドポイント一覧

**基盤 API**

| メソッド | パス | 説明 |
|---|---|---|
| GET | `/health` | ヘルスチェック |
| GET | `/api/v1/series/search?q=...` | Series 検索 |
| GET | `/api/v1/series/{series_id}` | Series メタデータ取得 |
| POST | `/api/v1/observations/fetch` | 観測値を FRED から取得・保存 |
| POST | `/api/v1/panel/build` | Macro panel 構築 |
| POST | `/api/v1/factors/compute` | 因子計算 |
| POST | `/api/v1/regimes/tag` | レジームタグ付け |
| POST | `/api/v1/releases/fetch` | Release calendar 取得 |
| GET | `/api/v1/releases/list` | Release calendar 一覧 |
| POST | `/api/v1/memo/generate` | 研究メモ生成・保存 |
| GET | `/api/v1/memo/list` | メモ一覧 |
| GET | `/api/v1/memo/{memo_id}/markdown` | メモを Markdown で取得 |
| GET | `/api/v1/ohlc/daily/{pair}` | 日足 OHLC 取得 |
| GET | `/api/v1/ohlc/intraday/{pair}/{timeframe}` | イントラデイ OHLC 取得 |
| GET | `/api/v1/ohlc/summary` | ロード済み OHLC サマリー |

**クロスアセット API（v1.4）**

| メソッド | パス | 説明 |
|---|---|---|
| POST | `/api/v1/market/uploads` | OHLC CSV アップロード（非同期）|
| GET | `/api/v1/market/uploads/{upload_id}` | アップロード状態確認 |
| GET | `/api/v1/market/uploads` | 最近のアップロード履歴 |
| GET | `/api/v1/market/bars` | 正規化バー取得 |
| POST | `/api/v1/market/bars/rebuild-daily` | 日次バー再構築 |
| GET | `/api/v1/quality/market-report` | Market Bars 品質レポート |
| POST | `/api/v1/cross-asset/features/rebuild` | クロスアセット特徴量再計算 |
| GET | `/api/v1/cross-asset/features` | クロスアセット特徴量取得 |
| GET | `/api/v1/panels/fx-crossasset` | FX クロスアセットパネル取得 |
| POST | `/api/v1/panels/fx-crossasset/rebuild` | パネル再構築 |

### 7.2 レスポンス共通形式

```json
{
  "status": "ok",
  "as_of": "2026-03-15T09:00:00",
  "timezone": "UTC",
  "data": { ... },
  "warnings": [],
  "errors": []
}
```

### 7.3 クロスアセット API 詳細（v1.4）

**POST `/api/v1/market/uploads`**

| パラメータ | 型 | 説明 |
|---|---|---|
| `file` | multipart | OHLC CSV ファイル |
| `instrument_id` | query | 例: `usatechidxusd_h4` |
| `timeframe` | query | `h4` / `h1` / `d1` |
| `delimiter` | query | `\t`（タブ）/ `,` / `;` |
| `has_header` | query | `true` / `false` |
| `ts_format` | query | 例: `%Y.%m.%d %H:%M` |

レスポンス 202: `{ "upload_id": "...", "status": "accepted", "next": "/market/uploads/{id}" }`

**GET `/api/v1/panels/fx-crossasset`**

| パラメータ | 型 | 説明 |
|---|---|---|
| `pair` | query | `USDJPY` / `EURUSD` / `AUDUSD` |
| `start` | query | 開始日（YYYY-MM-DD）|
| `end` | query | 終了日（YYYY-MM-DD）|
| `include_usatech` | query | USATECH 列を含む（デフォルト: true）|
| `include_fred` | query | VIX / USD Broad 等 FRED 列を含む（デフォルト: true）|

---

## 8. データ管理

### 8.1 ディレクトリ構成

```
fred-fx-research/
  data/
    fred_fx.duckdb          ← メインデータベース（全スキーマ）
    raw/                    ← FRED 生データ (Parquet)
      series_id=DEXJPUS/
        year=2026/
          <batch_id>.parquet
      uploads/              ← アップロード CSV の一時保存（v1.4）
    normalized/             ← 正規化済みデータ
    derived/                ← 計算済み因子・パネル・レジーム
    export/                 ← Parquet エクスポート先（v1.4）
      usatech/
        norm/               ← fact.market_bars_norm のエクスポート
        daily/              ← fact.market_bars_daily のエクスポート
    memos/                  ← 研究メモ JSON
```

### 8.2 DuckDB テーブル一覧

```sql
-- 全テーブル確認
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
tables = conn.execute(\"SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2\").fetchall()
for t in tables: print(t)
"
```

**main スキーマ（従来テーブル）**

| テーブル | 説明 |
|---|---|
| dim_series_registry | Series メタデータ・ドメイン・鮮度 |
| fact_series_observations_raw | FRED 生観測値（上書き禁止） |
| fact_series_vintages | 改定履歴 |
| fact_release_calendar | リリース日カレンダー |
| fact_market_series_normalized | 正規化済み時系列 |
| fact_derived_factors | 計算済み因子 |
| fact_ohlc_intraday | CSV OHLC データ (1min〜日足) |
| audit_ingestion_runs | 取り込み監査ログ |
| memo_store | 研究メモ |

**クロスアセットスキーマ（v1.4）**

| テーブル | スキーマ | 説明 |
|---|---|---|
| dim_market_instrument | md | インスツルメントマスタ |
| fact_upload_audit | ops | CSV アップロード監査ログ |
| market_bars_raw | fact | Market Bars 生データ（append-only） |
| market_bars_norm | fact | 正規化済み Market Bars |
| market_bars_daily | fact | 日次集約 Market Bars |
| cross_asset_feature_daily | fact | クロスアセット特徴量（long-form）|
| fx_cross_asset_daily_panel | mart | FX クロスアセットパネル（wide-form）|

### 8.3 クロスアセット行数確認

```bash
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
for tbl in ['fact.market_bars_raw', 'fact.market_bars_norm', 'fact.market_bars_daily',
            'fact.cross_asset_feature_daily', 'mart.fx_cross_asset_daily_panel']:
    n = conn.execute(f'SELECT count(*) FROM {tbl}').fetchone()[0]
    print(f'{tbl}: {n:,} rows')
"
```

### 8.4 Parquet エクスポート

```bash
python -c "
from app.storage.duckdb import get_connection
get_connection()
from app.services import market_bar_service
result = market_bar_service.export_to_parquet(
    'usatechidxusd_h4',
    'data/export/usatech'
)
print(result)
"
```

### 8.5 データのリセット

```bash
# 全データを削除（注意: 元に戻せません）
rm -rf data/raw data/normalized data/derived data/memos data/export data/fred_fx.duckdb

# FRED データ再取り込み
python jobs/backfill_series.py --start 2020-01-01

# USATECH 再ロード
python jobs/load_usatech_csv.py --data-dir /path/to/data/us_index --timeframes 240
python jobs/rebuild_cross_asset_daily.py --instrument-id usatechidxusd_h4 --pairs USDJPY,EURUSD,AUDUSD
```

---

## 9. トラブルシューティング

### 9.1 UI に接続できない

```bash
lsof -i :8501
streamlit run ui/Home.py --server.port 8501 --server.address 0.0.0.0 --server.headless true &
```

---

### 9.2 "API サーバーに接続できません" と表示される

```bash
curl http://localhost:8000/health
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &
```

---

### 9.3 パネルに "データがありません" と表示される

**Pair Workspace の場合**

```bash
python jobs/backfill_series.py --start 2020-01-01
```

**Cross Asset Explorer の場合（v1.4）**

```bash
# 1. USATECH CSV がロード済みか確認
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
n = conn.execute('SELECT count(*) FROM fact.market_bars_daily').fetchone()[0]
print(f'market_bars_daily: {n} rows')
"

# 2. 0 rows なら CSV をロード後、特徴量を再構築
kill $(lsof -t -i:8000)
python jobs/load_usatech_csv.py --data-dir /path/to/data/us_index --timeframes 240
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 &
python jobs/rebuild_cross_asset_daily.py --instrument-id usatechidxusd_h4 --pairs USDJPY,EURUSD,AUDUSD
```

---

### 9.4 FRED API エラー（429 Too Many Requests）

自動リトライが入っているため、しばらく待つと復旧します。FRED の制限は 2 req/sec です。

---

### 9.5 DuckDB ロックエラー（CSV ロード時）

**症状**: `IOException: Could not set lock on file`

**対処**

```bash
# 1. API サーバーを停止
kill $(lsof -t -i:8000)

# 2. CSV ロードを実行
python jobs/load_usatech_csv.py --data-dir /path/to/data --timeframes 240

# 3. API サーバーを再起動
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload &
```

---

### 9.6 アップロードステータスが "processing" のまま止まる

```bash
# アップロード監査ログでエラーを確認
python -c "
from app.storage.duckdb import get_connection
conn = get_connection()
rows = conn.execute('SELECT upload_id, status, error_message FROM ops.fact_upload_audit ORDER BY started_at DESC LIMIT 5').fetchall()
for r in rows: print(r)
"
```

エラーメッセージを確認し、CSV フォーマット（区切り文字・タイムスタンプ形式）を見直してください。

---

### 9.7 ログの確認

```bash
tail -f /tmp/api.log   # API ログ
tail -f /tmp/ui.log    # UI ログ
```

---

## 10. 用語集

| 用語 | 説明 |
|---|---|
| **FRED** | Federal Reserve Economic Data。セントルイス連銀が提供する経済データ API |
| **ALFRED** | Archival FRED。vintage データを含む改定履歴付き FRED |
| **series_id** | FRED の系列識別子（例: DEXJPUS）|
| **vintage** | データが改定された時点の値。バックテストでは vintage を指定して「当時見えていた値」を使う |
| **domain** | データ種別（fx_spot / rate_3m / yield_10y / risk / commodity 等）|
| **pair** | 通貨ペア（USDJPY / EURUSD / AUDUSD）|
| **regime** | 相場の局面分類（risk_on/off / carry_positive / inverted など）|
| **factor** | 説明変数（金利差・カーブ差・モメンタム・ボラティリティなど）|
| **panel** | 複数 series を日付軸で結合した横断データ |
| **forward fill** | 欠測を直前の値で補完 |
| **freshness** | series の鮮度（ok / warning / reject）|
| **is_supplemental** | CSV close 値で FRED 欠測日を補完したデータを示すフラグ（TRUE = 補完データ）|
| **OHLC** | Open-High-Low-Close。ローソク足の 4 本値 |
| **timeframe** | OHLC の足の長さ（分単位: 1, 5, 15, 30, 60, 240, 1440）|
| **REER** | Real Effective Exchange Rate。BIS 発表の実質実効為替レート |
| **policy_spread** | 2 国間の政策金利差 |
| **VIX** | CBOE Volatility Index。株式市場の将来ボラティリティ期待値。risk-off の proxy |
| **USD Broad** | 主要貿易相手国通貨に対する USD の加重平均実効レート（DTWEXBGS）|
| **DuckDB** | 列指向の組み込み分析 DB。Parquet を直接クエリできる |
| **Polars** | Python の高速 DataFrame ライブラリ（Rust 実装）|
| **USATECH** | USATECHIDXUSD — US Tech 100 指数（CFD）。クロスアセット分析の説明変数（v1.4）|
| **instrument_id** | Market Bars のインスツルメント識別子（例: `usatechidxusd_h4`）（v1.4）|
| **rv_20d** | Realized Volatility 20日。日次リターンの 20日ローリング標準偏差 × √252（年率換算）（v1.4）|
| **drawdown_20d** | 20日ドローダウン。close / rolling_max(close, 20) - 1（v1.4）|
| **corr_20d** | 20日ローリング相関係数。USATECH ret と FX ret の相関（v1.4）|
| **beta_60d** | 60日ローリング β。FX ret の USATECH ret に対する感応度（v1.4）|
| **risk_filter_flag** | No-trade フィルター。4 条件（DD / RV / VIX / USD-Z）のうち 2 つ以上成立で "avoid"（v1.4）|

---

*本マニュアルは FRED FX Research Workbench v1.4 に対応しています。*
