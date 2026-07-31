import os
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Azure CSP Billing Portal"
    environment: str = "development"
    secret_key: str = "change-me"
    session_cookie_name: str = "csp_portal_session"
    session_https_only: bool | None = None
    cors_origins: str = ""
    seed_default_accounts: bool | None = None
    database_url: str = "sqlite:///./data/app.db"
    duckdb_path: Path = Path("data/warehouse/csp.duckdb")
    uploads_dir: Path = Path("data/uploads")
    processed_dir: Path = Path("data/warehouse")
    chunk_size: int = 150_000
    max_upload_size_mb: int = 350
    default_vat: float = 1.12
    duckdb_threads: int = max(1, os.cpu_count() or 1)
    duckdb_memory_limit: str = "2048MiB"
    duckdb_temp_directory: Path = Path("data/warehouse/tmp")
    polars_infer_rows: int = 512
    polars_row_group_size: int = 256_000

    # List of Customer names that are VAT Exempt (case-insensitive partial match)
    vat_exempt_customers: list[str] = [
        "United Nations",
        "World Health Organization",
        "Asian Development Bank",
        "International Rice Research Institute",
        "Embassy",
        "Consulate"
    ]

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @field_validator("environment")
    @classmethod
    def normalize_environment(cls, value: str) -> str:
        return (value or "development").strip().lower()

    @property
    def parsed_cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @model_validator(mode="after")
    def apply_environment_defaults(self) -> "Settings":
        is_production = self.environment in {"prod", "production"}
        if self.session_https_only is None:
            self.session_https_only = is_production
        if self.seed_default_accounts is None:
            self.seed_default_accounts = not is_production
        if is_production and self.secret_key == "change-me":
            raise ValueError("SECRET_KEY must be set to a non-default value in production.")
        if is_production and not self.parsed_cors_origins:
            raise ValueError("CORS_ORIGINS must be set in production.")
        return self


settings = Settings()
