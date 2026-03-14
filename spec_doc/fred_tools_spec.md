FRED/ALFRED の取得仕様と、USD/JPY・EUR/USD・AUD/USD 向けに実用性の高い系列候補を確認しつつ、研究・バックテストにそのまま使える仕様書へ整理します。

次は、頻度差を踏まえた取得設計と、金利差・インフレ差・景気差・イベント差を含む補助マクロ系列を、安定運用できる具体的な series ID まで絞り込みます。

Thought for 27m 27s

以下を 「FRED API を使った FX マーケット分析データ仕様書 v1.0」 として定義します。対象は USD / JPY / EUR / AUD を中心にした日次〜月次のマクロ・金利・レジーム分析です。前提として、FRED には series 単位の取得に向く API v1 と、release 単位で全 series をまとめて取る API v2 があり、ALFRED 連携で real-time period と vintage を扱えます。fred/series/observations では units、frequency、aggregation_method、vintage_dates、output_type を指定でき、realtime_start / realtime_end の既定は「今日」です。

1. 目的

この仕様の目的は、FRED/ALFRED を使って 「どの通貨ペアが、どのレジームで、なぜ動きやすいか」 を日次〜月次で説明できるデータ基盤を作ることです。用途は、USDJPY・EURUSD・AUDUSD を中心とした 政策乖離、金利差、イールドカーブ差、リスクオン/オフ、商品市況、イベント前後 の研究です。FRED の主要 FX 系列は H.10 の New York 正午の buying rates で、日次レジーム分析には向きますが、ティック・秒足・執行分析には向きません。

2. 適用範囲と非対象

本仕様の対象は 日次・週次・月次・四半期 の上流研究です。対象外は、ティックデータ、板情報、約定品質分析、スリッページ推定、秒単位イベントトレード、ブローカー執行制御です。FRED の series/observations は低頻度への集約をサポートしますが、高頻度化はできません。したがって、本基盤は Research / Regime Desk 用 であり、Execution Engine 用ではありません。

3. 採用する FRED/ALFRED 機能
3.1 必須エンドポイント

fred/series/search: 系列探索。search_text、order_by、tag_names、exclude_tag_names を使えるので、ハードコードより先に series registry を組むのに使います。

fred/series: 系列メタデータ取得。

fred/series/observations: 観測値取得。JSON を標準形式にし、units・frequency・aggregation_method・vintage_dates を内部 API に露出させます。FRED は json/xml/xlsx/csv(zip) を返せますが、内部基盤は JSON に統一します。

fred/series/vintagedates: いつ改定・更新が起きたかを取るために使います。

fred/releases/dates: リリース日一覧。イベント研究の土台ですが、release date は FRED/ALFRED 上の実際の利用可能時刻を保証しない ので、時刻精度が必要な研究では別管理が要ります。

fred/v2/release/observations: release 単位で全 series を bulk 取得します。next_cursor でページングできるので、CPI や GDP などの release スナップショット収集に向きます。

3.2 認証とレート制御

v1 では API key は api_key パラメータで渡し、すべての web service request に API key が必要です。v2 はドキュメント例で HTTP header 認証を前提にしており、エラー仕様では 429 の前に毎秒 2 リクエストまで が明示されています。内部ツールは v1/v2 とも 2 req/sec 以下 に抑え、429・5xx に指数バックオフを入れます。

4. データ仕様
4.1 正規化の基本方針

FRED の FX 系列は通貨ごとに クオート方向が統一されていない ので、生の値をそのまま分析に流してはいけません。内部の canonical format は必ず
pair, base_ccy, quote_ccy, price_quote_per_base
に正規化します。生データは raw のまま保存し、正規化値を別列で持ちます。

4.2 必須データブロック

A. FX スポット（必須）

DEXJPUS: Japanese Yen to One U.S. Dollar。これはそのまま USDJPY として使えます。日次、H.10、New York 正午 buying rate です。

DEXUSEU: U.S. Dollars to One Euro。これはそのまま EURUSD として使えます。日次、H.10、New York 正午 buying rate です。

DEXUSAL: U.S. Dollars to One Australian Dollar。これはそのまま AUDUSD として使えます。日次、H.10、New York 正午 buying rate です。

B. USD アンカー（強く推奨）

DTWEXBGS: Nominal Broad U.S. Dollar Index。個別通貨ペアの分析でも「ドル全体が強いのか」を分離するために必須です。

C. フロントエンド金利 / キャリー（必須）

米国: EFFR を優先、補助で FEDFUNDS。EFFR は日次で、FEDFUNDS は月次の代表系列です。

ユーロ圏: ECBDFR（Deposit Facility Rate）、ECBMRRFR（Main Refinancing Operations Rate）。どちらも ECB の政策スタンス把握に使えます。

日本: IRSTCI01JPM156N（Call Money / Interbank Rate）を短期金利 proxy として使います。日本の旧 discount-rate 系列 INTDSRJPM193N は観測が 2017 年で止まり、更新も 2019 年なので、主系列には不適です。

豪州: IRSTCI01AUM156N（Call Money / Interbank Rate）を短期金利 proxy として使います。

D. 3M / 10Y カーブ（必須）

3M: IR3TIB01USM156N, IR3TIB01JPM156N, IR3TIB01EZM156N, IR3TIB01AUM156N。短期金利差と carry proxy の主力です。

10Y: IRLTLT01USM156N, IRLTLT01JPM156N, IRLTLT01EZM156N, IRLTLT01AUM156N。長期金利差と curve divergence を見るための主力です。

E. リスク・ストレス・商品（推奨）

VIXCLS: 株式市場の近接将来ボラ期待。JPY の safe-haven 仮説や AUD の risk-on/off proxy に有効です。

STLFSI4: 18 系列から構成される週次の金融ストレス指数。リスク縮小局面のレジーム変数に向きます。

DCOILWTICO: WTI。AUD の商品感応度をざっくり見る proxy として採用可能です。

F. バランスシート / 非伝統政策（推奨）

JPNASSETS: Bank of Japan: Total Assets for Japan。USDJPY の BoJ レジーム把握には強い補助系列です。

G. 実効為替レート / バリュエーション（推奨）

NBUSBIS, RBJPBIS, RBAUBIS, RBXMBIS などの BIS effective exchange rate indices は、単純な bilateral spot だけでは見えない「割高・割安」や slow-moving regime anchor を与えます。BIS 系の real effective exchange rates は、相対 CPI で調整した加重 bilateral average です。

4.3 インフレ・活動データの扱い

インフレ系列は 一律に 1 本へ固定しない 仕様にします。理由は、FRED 内の国別 CPI 系列の頻度と鮮度がかなり不均一だからです。たとえば、ユーロ圏 HICP は月次で 2025 年末まで更新されていますが、豪州の OECD CPI growth 系列は四半期、FRED 検索で見える日本の一部 OECD CPI 月次系列は 2021 年半ばで止まっています。したがって v1 では、インフレ block に freshness gate（last_updated, observation_end, frequency）を持たせ、国別 override を許可します。

5. 内部の canonical schema

最低限、以下の列を raw layer と normalized layer に分けて持ちます。

raw_observations

source_system = fred / alfred

series_id

title

date

value_raw

value_num

units_native

frequency_native

realtime_start

realtime_end

vintage_date nullable

release_id nullable

retrieved_at_utc

source_last_updated nullable

next_release_date nullable

normalized_market_series

series_id

obs_date

domain (fx_spot, policy_rate, rate_3m, yield_10y, risk, commodity, balance_sheet, effective_fx)

base_ccy

quote_ccy

pair

price_quote_per_base

is_derived

transformation

frequency_requested

aggregation_method

freshness_status

derived examples

EURJPY = EURUSD * USDJPY

AUDJPY = AUDUSD * USDJPY

usd_jpy_3m_spread = us_3m - jp_3m

aud_us_10y_spread = au_10y - us_10y

us_curve = us_10y - us_3m

ここは設計判断です。重要なのは raw を絶対に上書きしないこと と、通貨の向きが違う series を正規化せずに混ぜないこと です。

6. 取得・加工ルール

series registry を先に作る
　series/search を使って対象 series を登録し、tag_names や search_text で探索可能にします。series ID の完全ハードコードは最小限にします。

raw と normalized を分離する
　FRED の raw 値と、内部で解釈済みの normalized 値を別テーブルに置きます。欠損 "." は raw で保持しつつ numeric 側は NULL にします。FRED の examples でも "." は欠測として現れます。

頻度変換は FRED の機能を明示的に使う
　frequency は日次→週次/月次/四半期への低頻度集約に使え、aggregation_method は avg/sum/eop を持ちます。FX レベルは原則 eop、金利やストレス指数は avg を既定値にするのが妥当です。

バックテスト・イベント研究では vintage を強制する
　マクロ系列の join は、通常モードでは realtime_start/end=today、検証モードでは vintage_dates または real-time period を必須にします。series/vintagedates で revision history を取り、必要なら output_type=4 で initial release only を使います。

release study は日付と可用時刻を分ける
　releases/dates はイベント一覧の土台として使いますが、正確な availability timestamp には使いません。release-centric backfill は v2/release/observations を使い、実運用では別の availability_lag テーブルを持ちます。

7. 有効な分析手法
7.1 金利差 / キャリー回帰

最も有効なのは、USDJPY・EURUSD・AUDUSD を フロントエンド金利差 と 3M/10Y スプレッド差 に回帰させる手法です。

USDJPY: us_3m - jp_3m, us_10y - jp_10y, JPNASSETS, VIX

EURUSD: us_3m - ez_3m, us_10y - ez_10y, DTWEXBGS

AUDUSD: au_3m - us_3m, au_10y - us_10y, VIX, WTI

これは「何 bp の金利差変化に、どのペアがどれだけ反応しやすいか」を見る基本手法です。仮説 としては非常に強いですが、pair ごとの安定性は rolling で検証する前提です。

7.2 ローリング回帰 / Kalman beta

固定係数ではなく、90 日〜252 日の rolling OLS、または Kalman filter で
spot_return ~ rate_spread + VIX + USD_index + oil
の係数がどう変化するかを追います。通貨はレジームで反応係数が変わるので、これが一番「今どの説明変数が効いているか」を見やすいです。

7.3 レジーム分類

VIXCLS, STLFSI4, DTWEXBGS, 3M/10Y スプレッド差、FX 実現ボラを使って、

risk-on / risk-off

carry-dominant / policy-dominant

USD-broad-trend / idiosyncratic-cross
のレジームへ分類します。方法は HMM、Bayesian change point、単純な k-means でもよいですが、説明しやすさ を優先するならまず rule-based regime の方が良いです。

7.4 イベント・ウィンドウ分析

FRED/ALFRED は release calendar と vintage を持つので、

FOMC 周辺の USDJPY

ECB rate day の EURUSD

BoJ 周辺の USDJPY

CPI / GDP / 雇用統計 release 周辺
の pre/post window を作れます。ここでは 「当日反応」よりも、前後 1〜5 営業日の drift」 を見るのが FRED 向きです。release date が FRED availability と一致しない可能性があるので、時刻精度が必要な event surprise 研究は v1 の対象外にします。

7.5 バリュエーション・アンカー

BIS の effective exchange rate index を slow-moving anchor にして、spot がそこからどれだけ乖離しているかを見る手法は有効です。短期売買シグナルというより、「今の相場が carry で説明されているのか、バリュエーションから大きく離れているのか」 を判定する用途です。

8. 必要な API Tool

この仕様で必要な内部ツールは次の 10 本です。

8.1 fred_series_search_tool

役割: 系列探索
入力: search_text, tag_names, exclude_tag_names, order_by, limit
出力: 候補 series 一覧、頻度、units、source
根拠: FRED の search endpoint はこれらの検索パラメータを持ちます。

8.2 fred_series_metadata_tool

役割: series の title / notes / frequency / units / source / release を取得
入力: series_id
出力: メタデータ辞書
用途: freshness audit と citation metadata の作成

8.3 fred_observations_tool

役割: 観測値取得
入力: series_id, observation_start, observation_end, units, frequency, aggregation_method, realtime_start, realtime_end, vintage_dates, output_type
出力: raw observations
根拠: series/observations の主要パラメータをそのまま内部 API に写すのが一番安全です。

8.4 fred_vintage_tool

役割: revision / release 履歴の取得
入力: series_id, realtime_start, realtime_end
出力: vintage date 配列
根拠: series/vintagedates。

8.5 fred_release_calendar_tool

役割: リリース日一覧の取得
入力: start_date, end_date, include_release_dates_with_no_data
出力: release calendar
注意: availability timestamp ではない。

8.6 fred_release_bulk_tool

役割: release 単位 bulk 取得
入力: release_id, next_cursor
出力: series 群 + observations 群
注意: next_cursor でページ継続、v2 レート制限順守。

8.7 fx_quote_normalizer_tool

役割: raw FX series を canonical pair に変換
入力: series_id, value_raw, date
出力: pair, base_ccy, quote_ccy, price_quote_per_base
必須ロジック:

DEXJPUS -> USDJPY

DEXUSEU -> EURUSD

DEXUSAL -> AUDUSD

derived crosses を生成

8.8 macro_panel_builder_tool

役割: 複数 series を同一日付軸へ結合
入力: pair, date range, frequency
出力: feature-ready panel
仕様:

raw join と vintage-safe join を切替可能

NULL と欠測率を返す

8.9 spread_curve_engine_tool

役割: rate differentials / curve slopes / balance-sheet growth / broad-dollar changes を計算
入力: panel
出力: factor series
例:

us_jp_3m_spread

us_ez_10y_spread

jpnassets_yoy

usd_broad_20d_change

8.10 freshness_audit_tool

役割: stale / sparse / discontinued 系列を弾く
入力: series metadata + observations
出力: ok / warning / reject
必須ルール:

observation_end が遠すぎる series は reject

更新停止系列は補助扱いに格下げ

四半期・年次を日次モデルへ直接入れない

9. 非機能要件

raw responses は immutable に保存

すべての系列に citation metadata を保持

UTC で取得時刻を保存し、表示時のみ JST へ変換

v2 は 2 req/sec 以下、429/5xx は backoff

すべての研究結果に as_of, source, vintage_mode, missing_rate を添付

inflation / activity 系列は freshness gate を通過しない限りモデルへ入れない

旧 series や discontinued series は自動で warning

これで、「FRED をそのまま雑に読む」のではなく、「通貨方向、改定、頻度、鮮度を管理した FX レジーム研究基盤」 になります。

10. 実装優先順位

最初に作るべき順番は次です。

fred_series_search_tool

fred_observations_tool

fx_quote_normalizer_tool

macro_panel_builder_tool

spread_curve_engine_tool

freshness_audit_tool

fred_vintage_tool

fred_release_calendar_tool

fred_release_bulk_tool

この順なら、最短で USDJPY / EURUSD / AUDUSD の daily regime panel まで到達できます。

次段では、この仕様をそのまま OpenAPI YAML と DuckDB テーブル定義 に落とします。