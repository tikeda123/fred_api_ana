# FRED FX Research Workbench

FRED（Federal Reserve Economic Data）API を活用した FX マーケットの**マクロ・金利・レジーム・クロスアセット分析**基盤です。
USD/JPY・EUR/USD・AUD/USD を中心に、データ取得から特徴量計算・可視化までをワンストップで提供します。

## 主な機能

| 機能 | 説明 |
|---|---|
| FRED データ取得 | FX スポット・政策金利・市場金利・VIX・BIS REER・商品データを自動取得 |
| CSV OHLC 取込 | ブローカー CSV（1分足〜日足）のロード、FRED 欠測日の close 値自動補完 |
| USATECH クロスアセット | US Tech 100 Index (H4) × FX の相関・β・ドローダウン・risk_filter_flag |
| ファクター計算 | 金利差・REER スプレッド・Parkinson Vol・Daily Range・Overnight Gap 等 |
| レジーム分析 | risk_on / risk_off・carry 状態・カーブ形状の自動タグ付け |
| イベント分析 | FOMC 等リリース周辺の FX リターン観測 |
| 研究メモ | 仮説・観測・不確実性を構造化して保存・Markdown 出力 |

## 技術スタック

- **API**: FastAPI + Uvicorn
- **UI**: Streamlit + Plotly
- **DB**: DuckDB（組み込み OLAP）
- **データ処理**: Polars
- **言語**: Python 3.11+

## クイックスタート

```bash
# プロジェクトディレクトリへ移動
cd fred-fx-research

# 1. インストール
make install

# 2. FRED データのバックフィル（初回のみ）
make backfill

# 3. API サーバー起動（http://localhost:8000）
make api

# 4. Streamlit UI 起動（http://localhost:8501）
make ui

# API + UI を同時起動
make dev
```

### FRED API キー

`.env` ファイルに FRED API キーを設定してください。

```
FRED_API_KEY=your_api_key_here
```

## プロジェクト構成

```
fred-fx-research/
├── app/
│   ├── api/routers/      # FastAPI エンドポイント
│   ├── services/         # ビジネスロジック
│   ├── storage/          # DuckDB / Parquet I/O
│   ├── models/           # Pydantic / ドメインモデル
│   └── core/             # 設定・ロギング・例外
├── ui/
│   ├── Home.py           # Streamlit エントリポイント
│   └── pages/            # 11 ページの分析画面
├── jobs/                 # バッチジョブ（backfill / daily 等）
├── sql/                  # DDL スキーマ定義
├── tests/                # unit / integration / e2e テスト
├── docs/                 # ドキュメント
└── data/                 # DuckDB + Parquet（gitignore）
```

## UI 画面一覧

| # | 画面名 | 概要 |
|---|--------|------|
| 00 | Home | スポットレート・レジーム・最終更新サマリー |
| 01 | Series Catalog | FRED 系列の検索・メタデータ閲覧 |
| 02 | Pair Workspace | 通貨ペアの多段チャート（金利差・Vol・Range・レジーム） |
| 03 | Regime Dashboard | risk_on/off・carry 状態のタイムライン |
| 04 | Event Study | FOMC 等リリース前後の FX リターン観測 |
| 05 | Data Quality | データ鮮度・欠測・品質レポート |
| 06 | Memo Builder | 研究メモの作成・閲覧・Markdown 出力 |
| 07 | Data Upload | OHLC CSV ファイルのアップロード・ステータス管理 |
| 08 | Cross Asset Explorer | FX × USATECH オーバーレイ分析（Vol / DD / Risk Filter） |
| 09 | Lag Correlation | FX × USATECH のローリング相関・ラグ相関・散布図 |
| 10 | Filter Lab | Risk Filter の閾値調整・PnL シミュレーション |
| 11 | Releases | FRED Release Calendar の取得・閲覧 |

## API エンドポイント（主要）

| メソッド | パス | 概要 |
|----------|------|------|
| GET | `/api/v1/series/{series_id}` | FRED 系列メタデータ |
| POST | `/api/v1/observations/fetch` | FRED データ取得・保存 |
| POST | `/api/v1/panel/build` | FX 分析パネル構築 |
| POST | `/api/v1/factors/compute` | ファクター計算 |
| POST | `/api/v1/regimes/tag` | レジームタグ付け |
| POST | `/api/v1/market/uploads` | CSV アップロード（multipart） |
| POST | `/api/v1/market/uploads/from-path` | CSV 取込（ファイルパス指定 / AI エージェント向け） |
| GET | `/api/v1/panels/fx-crossasset` | クロスアセットパネル取得 |
| POST | `/api/v1/panels/fx-crossasset/rebuild` | パネル全再構築 |

## Make コマンド

```
make install          依存パッケージをインストール
make api              FastAPI サーバーを起動 (port 8000)
make ui               Streamlit UI を起動 (port 8501)
make dev              API + UI を同時起動
make test             全テストを実行
make test-unit        unit テストのみ
make backfill         全 series を 2020-01-01 からバックフィル
make daily            デイリー更新バッチを実行
make load-csv         CSV OHLC をロード + FRED 補完
make lint             ruff でコードチェック
make clean-data       全データを削除
```

## ドキュメント

- [運用マニュアル](fred-fx-research/docs/operation_manual.md) — システム構成・画面操作ガイド・API リファレンス・データ管理・トラブルシューティング
- [API リファレンス](fred-fx-research/docs/api_reference.md) — 全 37 エンドポイントの詳細仕様・リクエスト/レスポンス例・AI エージェント向けワークフロー
- [改善計画](fred-fx-research/docs/improvement_plan.md) — 実装ステータス・優先度別改善項目・USATECH クロスアセット統合詳細
