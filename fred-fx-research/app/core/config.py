from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_env: str = "dev"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    fred_api_key: str
    fred_base_url: str = "https://api.stlouisfed.org/fred"

    duckdb_path: str = "./data/fred_fx.duckdb"
    raw_data_root: str = "./data/raw"
    normalized_data_root: str = "./data/normalized"
    derived_data_root: str = "./data/derived"

    default_tz: str = "UTC"
    streamlit_server_port: int = 8501

    # Rate control: FRED v1 = 2 req/sec max
    fred_max_rps: float = 2.0
    fred_retry_max_attempts: int = 5
    fred_retry_wait_min: float = 1.0
    fred_retry_wait_max: float = 30.0


settings = Settings()
