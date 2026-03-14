# FRED FX Research Workbench — 改善計画

作成日: 2026-03-14
監査基準: `fred_tools_spec.md` / `fred_fx_implementation_spec.md`

---

## 総合評価

**達成率約 98%** — 主要機能・データカバレッジ・CSV OHLC 補完・OHLC ファクター・JST 表示・UI・USATECH クロスアセット統合が稼働。CPI 等は保留

---

## 改善項目一覧

### 優先度: 高

#### H1. 政策金利の panel / factor 連携
- **現状**: EFFR, ECBDFR, IRSTCI01JPM156N, IRSTCI01AUM156N は FX_SERIES_MAP に定義済みだが、
  backfill の DEFAULT_SERIES に含まれておらず panel にも組み込まれていない。
- **改善**: backfill 対象に追加 + PAIR_SERIES_MAP に `us_policy` / `jp_policy` / `ez_policy` / `au_policy` 追加
  + factor_service に `us_jp_policy_spread` 等を追加
- **対象ファイル**: `jobs/backfill_series.py`, `app/storage/repositories/panel_repo.py`, `app/services/factor_service.py`

#### H2. `jpnassets_yoy` ファクター追加
- **現状**: JPNASSETS は panel に `boj_assets` として含まれているが YoY 変化率ファクターが未計算。
- **改善**: `factor_service.py` の USDJPY 向けに `jpnassets_yoy` を追加（252 日変化率）
- **対象ファイル**: `app/services/factor_service.py`

### 優先度: 中

#### M1. BIS REER 系列の追加
- **現状**: RBUSBIS, RBJPBIS, RBAUBIS, RBXMBIS が完全未実装。
  実質実効為替レートはスペックの「為替水準の歴史的位置付け」に必要。
- **改善**: FX_SERIES_MAP に domain=`reer` で追加、backfill 対象に追加、
  panel に `us_reer` / `jp_reer` 等として追加、`reer_spread` ファクター追加
- **対象ファイル**: `app/models/domain_models.py`, `jobs/backfill_series.py`,
  `app/storage/repositories/panel_repo.py`, `app/services/factor_service.py`

#### M2. `source_frequency` の正規化
- **現状**: `normalize_service.py` で `source_frequency` が常に空文字列。
- **改善**: normalize 時に `dim_series_registry.frequency_native` を参照して値を設定
- **対象ファイル**: `app/services/normalize_service.py`

#### M3. スキーマ列追加
- **現状**: `fact_series_observations_raw` に `source_last_updated` 列なし。
  ingest 時に FRED の `last_updated` を保存できない。
- **改善**: schema.sql に列追加 + duckdb.py の init で ALTER TABLE IF NOT EXISTS を実行
  + ingest_service.py で保存
- **対象ファイル**: `sql/schema.sql`, `app/storage/duckdb.py`, `app/services/ingest_service.py`

#### M4. JST タイムゾーン表示（v1.3）
- **現状**: UTC のみ。スペックは「UTC で保存、JST で表示」を要求。
- **改善**: `ui/tz_utils.py` に JST 変換ヘルパーを追加。
  Home（as of 表示）、Regime Dashboard（最終更新）、Data Quality（ingestion log・registry）、
  Memo Builder（created_at・Markdown プレビュー）の日時表示を JST に変換。
  API レスポンスの `as_of` は UTC のまま保存（スペック通り）。
- **対象ファイル**: `ui/tz_utils.py` (新規), `ui/Home.py`, `ui/pages/03_Regime_Dashboard.py`,
  `ui/pages/05_Data_Quality.py`, `ui/pages/06_Memo_Builder.py`

#### M5. CSV OHLC ローダー・FRED 欠測補完（v1.2）
- **現状**: FRED は日足 close のみ、祝日欠測・T+1-2 ラグあり。イントラデイデータなし。
- **改善**: ブローカー CSV (1min〜日足) を `fact_ohlc_intraday` テーブルにロード。
  日足 close で FRED 欠測日を `is_supplemental=TRUE` として自動補完。
  OHLC API エンドポイント (daily / intraday / summary) を追加。
- **対象ファイル**: `app/services/csv_loader_service.py` (新規), `jobs/load_csv_ohlc.py` (新規),
  `app/api/routers/ohlc.py` (新規), `sql/schema.sql`, `app/storage/duckdb.py`,
  `app/services/normalize_service.py`, `Makefile`

#### M6. UI 全ページ改善（v1.2）
- **現状**: UI が v1.1 のバックエンド機能（政策金利・BIS REER・jpnassets_yoy）を反映していない。
- **改善**: Home（スポット値+レジーム）、Pair Workspace（5 段チャート・rolling correlation）、
  Regime Dashboard（サマリーカード）、Event Study（FOMC 日付拡張）、
  Data Quality（ペア選択・last_updated）、Memo Builder（feature リスト更新）を全面改修。
- **対象ファイル**: `ui/Home.py`, `ui/pages/02〜06` 全ページ

#### S2. Parkinson Vol + Daily Range ファクター追加（v1.3）
- **現状**: CSV OHLC データ（`fact_ohlc_intraday`）はロード済みだが、close-to-close 以外のボラティリティ因子がない。
- **改善**: `compute_ohlc_factors()` を追加し、Parkinson Vol 20d / Daily Range % / Daily Range MA20 / Overnight Gap を計算。
  `POST /factors/compute` に自動統合。Pair Workspace に Volatility / Range サブプロット追加。
- **対象ファイル**: `app/services/factor_service.py`, `app/api/routers/factors.py`,
  `ui/pages/02_Pair_Workspace.py`, `tests/unit/test_factors.py`

### 優先度: 低

#### L1. FRED v2 bulk endpoint
- **現状**: fred/v2/release/observations 未使用（現行の series ループで機能的に問題なし）
- **改善**: バックフィル高速化のため任意で実装

#### L2. CPI / インフレ系列
- **現状**: CPIAUCSL, JPNCPIALLMINMEI 等が未実装
- **改善**: FX 分析での直接利用場面が限定的のため、需要が出た時点で追加

#### L3. 引用メタデータ詳細化
- **現状**: `source_name` のみ。series_id + vintage_date + retrieved_at_utc の詳細引用が不足。
- **改善**: memo_service の sources リストに詳細情報を追加

---

## 実装ステータス

| ID | 項目 | ステータス | 実装日 |
|----|------|-----------|--------|
| H1 | 政策金利 panel/factor 連携 | ✅ 完了 | 2026-03-14 |
| H2 | jpnassets_yoy ファクター | ✅ 完了 | 2026-03-14 |
| M1 | BIS REER 系列 | ✅ 完了 | 2026-03-14 |
| M2 | source_frequency 正規化 | ✅ 完了 | 2026-03-14 |
| M3 | スキーマ列追加 | ✅ 完了 | 2026-03-14 |
| M4 | JST タイムゾーン表示 | ✅ 完了 | 2026-03-14 |
| M5 | CSV OHLC ローダー・FRED 欠測補完 | ✅ 完了 | 2026-03-14 |
| M6 | UI 全ページ改善 | ✅ 完了 | 2026-03-14 |
| S2 | Parkinson Vol + Daily Range ファクター | ✅ 完了 | 2026-03-14 |
| S3 | USATECH クロスアセット統合 (Phase 1-4) | ✅ 完了 | 2026-03-15 |
| L1 | v2 bulk endpoint | ⏳ 保留 | - |
| L2 | CPI/インフレ系列 | ⏳ 保留 | - |
| L3 | 引用メタデータ詳細化 | ⏳ 保留 | - |

---

## S3 USATECH クロスアセット統合 詳細（v1.4）

| Phase | 内容 | Step 数 | ステータス |
|---|---|---|---|
| Phase 1: スキーマ＆取り込み基盤 | DDL / DuckDB 初期化 / Pydantic モデル / バリデーション / リポジトリ / CSV 取り込み / Market Bar サービス / ルーター / テスト / 初期データ投入ジョブ | 12 | ✅ 完了 |
| Phase 2: 特徴量＆パネル | Cross Asset リポジトリ / 特徴量サービス / Join Panel サービス / ルーター / テスト / 再構築ジョブ | 6 | ✅ 完了 |
| Phase 3: Streamlit UI | Data Upload / Cross Asset Explorer / Lag Correlation / Filter Lab | 4 | ✅ 完了 |
| Phase 4: 品質＆運用 | Data Quality 画面拡張 / Parquet エクスポート / ドキュメント更新 | 3 | ✅ 完了 |

**新機能サマリー（v1.4）**

- **7 テーブル追加**: md / ops / fact / mart スキーマ
- **8 API エンドポイント追加**: uploads / bars / cross-asset features / fx-crossasset panel
- **4 Streamlit 画面追加**: 07_Data_Upload / 08_Cross_Asset_Explorer / 09_Lag_Correlation / 10_Filter_Lab
- **85 ユニットテスト**: 全 pass
- **risk_filter_flag**: 4 条件ルールベース（DD / RV / VIX / USD-Z）
- **Parquet エクスポート**: `market_bar_service.export_to_parquet()` でバックアップ対応
