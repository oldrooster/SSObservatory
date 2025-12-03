"""Application configuration helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass
class AzureConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    graph_base_url: str = "https://graph.microsoft.com/v1.0"
    scope: str = "https://graph.microsoft.com/.default"


@dataclass
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: Optional[str] = None


@dataclass
class AppConfig:
    azure: AzureConfig
    database: DatabaseConfig
    lookback_days: int = 30
    fetch_page_size: int = 100


def env(key: str, default: Optional[str] = None, required: bool = True) -> str:
    """Fetch and validate environment variables."""
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value  # type: ignore[return-value]


def get_config() -> AppConfig:
    azure = AzureConfig(
        tenant_id=env("AZURE_TENANT_ID"),
        client_id=env("AZURE_CLIENT_ID"),
        client_secret=env("AZURE_CLIENT_SECRET"),
        graph_base_url=os.getenv("GRAPH_BASE_URL", "https://graph.microsoft.com/v1.0"),
    )

    database = DatabaseConfig(
        host=env("PGHOST"),
        port=int(env("PGPORT", "5432", required=False)),
        dbname=env("PGDATABASE"),
        user=env("PGUSER"),
        password=env("PGPASSWORD"),
        sslmode=os.getenv("PGSSLMODE"),
    )

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "30"))
    fetch_page_size = min(999, max(1, int(os.getenv("GRAPH_PAGE_SIZE", "100"))))

    return AppConfig(
        azure=azure,
        database=database,
        lookback_days=lookback_days,
        fetch_page_size=fetch_page_size,
    )
