"""Application configuration helpers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
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
    service_principal_filter: str = "servicePrincipalType eq 'Application'"
    exclude_hidden_apps: bool = True
    local_exclude_owner_ids: List[str] = field(default_factory=list)
    local_exclude_publishers: List[str] = field(default_factory=list)


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

    raw_sslmode = os.getenv("PGSSLMODE")
    sslmode = raw_sslmode.strip() if raw_sslmode else None
    if not sslmode:
        os.environ.pop("PGSSLMODE", None)

    database = DatabaseConfig(
        host=env("PGHOST"),
        port=int(env("PGPORT", "5432", required=False)),
        dbname=env("PGDATABASE"),
        user=env("PGUSER"),
        password=env("PGPASSWORD"),
        sslmode=sslmode,
    )

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "30"))
    fetch_page_size = min(999, max(1, int(os.getenv("GRAPH_PAGE_SIZE", "100"))))
    sp_filter = os.getenv(
        "SERVICE_PRINCIPAL_FILTER",
        "servicePrincipalType eq 'Application'",
    )

    exclude_hidden = os.getenv("EXCLUDE_HIDE_APP_TAG", "true").strip().lower() in {"1", "true", "yes"}
    owner_ids = _split_csv(
        "EXCLUDE_OWNER_ORGANIZATION_IDS",
        default="f8cdef31-a31e-4b4a-93e4-5f571e91255a",
    )
    publishers = _split_csv("EXCLUDE_PUBLISHERS", default="Microsoft")

    return AppConfig(
        azure=azure,
        database=database,
        lookback_days=lookback_days,
        fetch_page_size=fetch_page_size,
        service_principal_filter=sp_filter,
        exclude_hidden_apps=exclude_hidden,
        local_exclude_owner_ids=owner_ids,
        local_exclude_publishers=publishers,
    )


def _split_csv(key: str, default: str = "") -> List[str]:
    raw = os.getenv(key, default)
    if not raw:
        return []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]
