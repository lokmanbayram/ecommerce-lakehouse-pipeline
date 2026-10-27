"""Centralized configuration and environment validation module."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _require_env(key: str, default: str | None = None) -> str:
    """Retrieve an environment variable or fail fast if missing."""
    value = os.getenv(key, default)
    if value is None or value.strip() == "":
        raise EnvironmentError(
            f"Missing required environment variable: '{key}'. "
            "Ensure it is defined in your local .env file or CI/CD secrets."
        )
    return value.strip()


@dataclass(frozen=True)
class S3Config:
    access_key: str
    secret_key: str
    region: str
    bucket_name: str


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class APIConfig:
    fx_base_url: str
    ecommerce_base_url: str
    request_timeout: int = 15
    max_retries: int = 3


def load_s3_config() -> S3Config:
    return S3Config(
        access_key=_require_env("AWS_ACCESS_KEY_ID"),
        secret_key=_require_env("AWS_SECRET_ACCESS_KEY"),
        region=_require_env("AWS_REGION", default="eu-central-1"),
        bucket_name=_require_env("S3_BUCKET_NAME"),
    )


def load_warehouse_config() -> WarehouseConfig:
    return WarehouseConfig(
        host=_require_env("DB_HOST"),
        port=int(_require_env("DB_PORT", default="5432")),
        database=_require_env("DB_NAME"),
        user=_require_env("DB_USER"),
        password=_require_env("DB_PASSWORD"),
    )


def load_api_config() -> APIConfig:
    return APIConfig(
        fx_base_url=_require_env("FX_API_BASE_URL", default="https://api.frankfurter.app"),
        ecommerce_base_url=_require_env("ECOMMERCE_API_BASE_URL", default="https://fakestoreapi.com"),
    )