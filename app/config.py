import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Azure CSP Billing Portal"
    secret_key: str = "change-me"
    session_cookie_name: str = "csp_portal_session"
    database_url: str = "sqlite:///./data/app.db"
    duckdb_path: Path = Path("data/warehouse/csp.duckdb")
    uploads_dir: Path = Path("data/uploads")
    processed_dir: Path = Path("data/warehouse")
    chunk_size: int = 150_000
    max_upload_size_mb: int = 350
    default_vat: float = 1.12
    duckdb_threads: int = max(1, os.cpu_count() or 1)
    polars_infer_rows: int = 512
    polars_row_group_size: int = 256_000

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
