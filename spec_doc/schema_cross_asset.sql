CREATE SCHEMA IF NOT EXISTS md;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS md.dim_market_instrument (
    instrument_id TEXT PRIMARY KEY,
    source_system TEXT NOT NULL,
    source_vendor TEXT,
    vendor_symbol TEXT NOT NULL,
    canonical_symbol TEXT NOT NULL,
    instrument_name TEXT,
    asset_class TEXT NOT NULL,
    venue TEXT,
    base_ccy TEXT,
    quote_ccy TEXT,
    timeframe_native TEXT NOT NULL,
    timezone_native TEXT NOT NULL DEFAULT 'UTC',
    file_format TEXT,
    delimiter TEXT,
    has_header BOOLEAN,
    ts_format TEXT,
    price_decimals INTEGER,
    volume_semantics TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.fact_upload_audit (
    upload_id TEXT PRIMARY KEY,
    instrument_id TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    source_file_size_bytes BIGINT,
    parser_name TEXT,
    parser_options_json TEXT,
    row_count_detected BIGINT,
    row_count_loaded BIGINT,
    row_count_rejected BIGINT DEFAULT 0,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
    source_line_hash TEXT NOT NULL,
    ingest_status TEXT DEFAULT 'loaded',
    quality_flags TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (upload_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS fact.market_bars_norm (
    instrument_id TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    ts_utc TIMESTAMP NOT NULL,
    trade_date_utc DATE NOT NULL,
    bar_year INTEGER NOT NULL,
    bar_month INTEGER NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    simple_ret_1bar DOUBLE,
    log_ret_1bar DOUBLE,
    hl_range_pct DOUBLE,
    oc_body_pct DOUBLE,
    gap_from_prev_close_pct DOUBLE,
    h4_slot_utc TEXT,
    session_bucket TEXT,
    is_weekend_gap BOOLEAN DEFAULT FALSE,
    quality_status TEXT DEFAULT 'ok',
    source_upload_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_id, timeframe, ts_utc)
);

CREATE TABLE IF NOT EXISTS fact.market_bars_daily (
    instrument_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    timeframe_source TEXT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    bar_count INTEGER,
    simple_ret_1d DOUBLE,
    log_ret_1d DOUBLE,
    range_pct_1d DOUBLE,
    gap_from_prev_close_pct DOUBLE,
    quality_status TEXT DEFAULT 'ok',
    build_id TEXT,
    built_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_id, obs_date)
);

CREATE TABLE IF NOT EXISTS fact.cross_asset_feature_daily (
    feature_scope TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    feature_group TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_horizon TEXT,
    feature_value DOUBLE,
    feature_value_text TEXT,
    source_instrument_id TEXT,
    source_table TEXT,
    build_id TEXT,
    built_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (feature_scope, scope_id, obs_date, feature_name, feature_horizon)
);

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

CREATE INDEX IF NOT EXISTS idx_market_bars_norm_inst_tf_ts
    ON fact.market_bars_norm (instrument_id, timeframe, ts_utc);

CREATE INDEX IF NOT EXISTS idx_market_bars_daily_inst_date
    ON fact.market_bars_daily (instrument_id, obs_date);

CREATE INDEX IF NOT EXISTS idx_cross_asset_feature_scope_date
    ON fact.cross_asset_feature_daily (feature_scope, scope_id, obs_date);

CREATE INDEX IF NOT EXISTS idx_fx_cross_asset_panel_pair_date
    ON mart.fx_cross_asset_daily_panel (pair, obs_date);
