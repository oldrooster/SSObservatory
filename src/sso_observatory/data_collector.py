"""Collect enterprise application usage data from Entra ID."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Sequence

from .config import AppConfig
from .graph_client import GraphClient

LOGGER = logging.getLogger(__name__)


@dataclass
class EnterpriseAppRecord:
    app_object_id: str
    app_id: Optional[str]
    display_name: str
    account_enabled: bool
    user_signins_last_30_days: int
    has_valid_certificate: bool
    nearest_cert_expiry: Optional[datetime]
    sampled_until: datetime


class EnterpriseAppCollector:
    """Coordinates Graph ingestion and database persistence."""

    def __init__(self, config: AppConfig, graph_client: GraphClient, database) -> None:
        self.config = config
        self.graph_client = graph_client
        self.database = database

    def run(self) -> None:
        batch: List[EnterpriseAppRecord] = []
        ingested = 0
        for record in self._build_records():
            batch.append(record)
            if len(batch) >= 100:
                self.database.upsert_apps(batch)
                ingested += len(batch)
                batch.clear()
        if batch:
            self.database.upsert_apps(batch)
            ingested += len(batch)
        LOGGER.info("Upserted %s enterprise app rows", ingested)

    def _build_records(self) -> Iterator[EnterpriseAppRecord]:
        sampled_until = datetime.now(timezone.utc)
        for sp in self._iter_service_principals():
            record = self._record_from_service_principal(sp, sampled_until)
            yield record

    def _iter_service_principals(self) -> Iterator[Dict]:
        params = {
            "$select": "id,appId,displayName,accountEnabled,keyCredentials",
            "$filter": "servicePrincipalType eq 'Application'",
            "$top": str(self.config.fetch_page_size),
        }
        yield from self.graph_client.paginate("/servicePrincipals", params=params)

    def _record_from_service_principal(
        self,
        sp: Dict,
        sampled_until: datetime,
    ) -> EnterpriseAppRecord:
        app_object_id = sp.get("id")
        if not app_object_id:
            raise ValueError("Service principal payload missing id")
        signin_count = self._fetch_signin_count(sp.get("appId"))
        has_valid_cert, nearest_expiry = analyze_certificates(sp.get("keyCredentials", []))
        return EnterpriseAppRecord(
            app_object_id=app_object_id,
            app_id=sp.get("appId"),
            display_name=sp.get("displayName", "<unknown>"),
            account_enabled=bool(sp.get("accountEnabled", True)),
            user_signins_last_30_days=signin_count,
            has_valid_certificate=has_valid_cert,
            nearest_cert_expiry=nearest_expiry,
            sampled_until=sampled_until,
        )

    def _fetch_signin_count(self, app_id: Optional[str]) -> int:
        if not app_id:
            return 0
        start_time = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
        start_iso = start_time.isoformat().replace("+00:00", "Z")
        params = {
            "$filter": f"appId eq '{app_id}' and createdDateTime ge {start_iso}",
            "$count": "true",
            "$top": str(self.config.fetch_page_size),
        }
        headers = {"ConsistencyLevel": "eventual"}
        payload = self.graph_client.get("/auditLogs/signIns", params=params, headers=headers)
        if "@odata.count" in payload:
            return int(payload.get("@odata.count", 0))
        total = len(payload.get("value", []))
        next_link = payload.get("@odata.nextLink")
        while next_link:
            page = self.graph_client.get(next_link, headers=headers, absolute=True)
            total += len(page.get("value", []))
            next_link = page.get("@odata.nextLink")
        return total


def analyze_certificates(key_credentials: Sequence[Dict]) -> tuple[bool, Optional[datetime]]:
    now = datetime.now(timezone.utc)
    has_valid = False
    nearest_expiry: Optional[datetime] = None
    for cred in key_credentials:
        if cred.get("type") != "AsymmetricX509Cert":
            continue
        expiry = parse_datetime(cred.get("endDateTime"))
        if not expiry:
            continue
        if expiry > now:
            has_valid = True
        if not nearest_expiry or expiry < nearest_expiry:
            nearest_expiry = expiry
    return has_valid, nearest_expiry


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except ValueError:
        LOGGER.warning("Unable to parse datetime: %s", value)
        return None

```}