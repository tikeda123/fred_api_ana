-- FRED FX Research Workbench - DuckDB Schema

CREATE TABLE IF NOT EXISTS dim_series_registry (
    series_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_name TEXT,
    release_id INTEGER,
    release_name TEXT,
    units_native TEXT,
    frequency_native TEXT,
    seasonal_adjustment TEXT,
    observation_start DATE,
    observation_end DATE,
    last_updated TIMESTAMP,
    notes TEXT,
    domain TEXT,
    base_ccy TEXT,
    quote_ccy TEXT,
    pair TEXT,
    quote_direction TEXT,
    freshness_status TEXT DEFAULT 'unknown',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_series_observations_raw (
    series_id TEXT NOT NULL,
    date DATE NOT NULL,
    value_raw TEXT NOT NULL,
    value_num DOUBLE,
    realtime_start DATE NOT NULL,
    realtime_end DATE NOT NULL,
    retrieved_at_utc TIMESTAMP NOT NULL,
    file_batch_id TEXT NOT NULL,
    source_last_updated TIMESTAMP,
    PRIMARY KEY (series_id, date, realtime_start, realtime_end)
);

CREATE TABLE IF NOT EXISTS fact_series_vintages (
    series_id TEXT NOT NULL,
    vintage_date DATE NOT NULL,
    retrieved_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (series_id, vintage_date)
);

CREATE TABLE IF NOT EXISTS fact_release_calendar (
    release_id INTEGER NOT NULL,
    release_name TEXT NOT NULL,
    release_date DATE NOT NULL,
    press_release BOOLEAN,
    link TEXT,
    retrieved_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (release_id, release_date)
);

CREATE TABLE IF NOT EXISTS fact_market_series_normalized (
    series_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    domain TEXT NOT NULL,
    pair TEXT,
    base_ccy TEXT,
    quote_ccy TEXT,
    value DOUBLE,
    units_normalized TEXT,
    is_derived BOOLEAN DEFAULT FALSE,
    is_supplemental BOOLEAN DEFAULT FALSE,
    transformation TEXT,
    source_frequency TEXT,
    frequency_requested TEXT,
    aggregation_method TEXT,
    as_of_realtime_start DATE,
    as_of_realtime_end DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (series_id, obs_date, as_of_realtime_start, as_of_realtime_end)
);

CREATE TABLE IF NOT EXISTS fact_derived_factors (
    factor_id TEXT NOT NULL,
    obs_date DATE NOT NULL,
    pair TEXT,
    factor_group TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    value DOUBLE,
    input_series_ids TEXT,
    formula TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (factor_id, obs_date, pair)
);

CREATE TABLE IF NOT EXISTS fact_ohlc_intraday (
    pair TEXT NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
    timeframe TEXT NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    source TEXT DEFAULT 'csv',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pair, datetime_utc, timeframe)
);

CREATE TABLE IF NOT EXISTS audit_ingestion_runs (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    endpoint TEXT,
    request_params TEXT,
    record_count INTEGER,
    error_message TEXT
);
