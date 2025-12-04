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
    app_description: Optional[str]
    app_owner_organization_id: Optional[str]
    app_role_assignment_required: Optional[bool]
    created_datetime: Optional[datetime]
    description: Optional[str]
    homepage: Optional[str]
    login_url: Optional[str]
    notes: Optional[str]
    notification_emails: Optional[List[str]]
    saml_sso_settings: Optional[Dict]
    preferred_single_sign_on_mode: Optional[str]
    tags: Optional[List[str]]


class EnterpriseAppCollector:
    """Coordinates Graph ingestion and database persistence."""

    def __init__(self, config: AppConfig, graph_client: GraphClient, database) -> None:
        self.config = config
        self.graph_client = graph_client
        self.database = database

    def run(self) -> None:
        LOGGER.info("Fetching enterprise apps from Microsoft Graph")
        service_principals = list(self._iter_service_principals())
        raw_total = len(service_principals)
        LOGGER.info("Discovered %s enterprise apps before local filtering", raw_total)
        if not service_principals:
            LOGGER.warning("No enterprise apps returned from Graph")
            return

        service_principals = self._apply_local_filters(service_principals)
        filtered_total = len(service_principals)
        LOGGER.info("Retained %s enterprise apps after local filtering", filtered_total)
        if not service_principals:
            LOGGER.warning("All enterprise apps were filtered out locally; nothing to process")
            return

        batch: List[EnterpriseAppRecord] = []
        ingested = 0
        sampled_until = datetime.now(timezone.utc)
        total = len(service_principals)
        for idx, sp in enumerate(service_principals, start=1):
            record = self._record_from_service_principal(sp, sampled_until)
            LOGGER.info("Processed %s (%s/%s)", record.display_name, idx, total)
            batch.append(record)
            if len(batch) >= 100:
                self.database.upsert_apps(batch)
                ingested += len(batch)
                batch.clear()
        if batch:
            self.database.upsert_apps(batch)
            ingested += len(batch)
        LOGGER.info("Upserted %s enterprise app rows", ingested)

    def _iter_service_principals(self) -> Iterator[Dict]:
        params = {
            "$filter": self.config.service_principal_filter,
            "$top": str(self.config.fetch_page_size),
        }
        headers = {"ConsistencyLevel": "eventual"}
        yield from self.graph_client.paginate("/servicePrincipals", params=params, headers=headers)

    def _apply_local_filters(self, service_principals: List[Dict]) -> List[Dict]:
        owner_blocklist = {oid.lower() for oid in self.config.local_exclude_owner_ids}
        publisher_blocklist = {pub.lower() for pub in self.config.local_exclude_publishers}
        filtered: List[Dict] = []
        for sp in service_principals:
            if self.config.exclude_hidden_apps and _has_hide_tag(sp):
                continue
            owner = str(sp.get("appOwnerOrganizationId", "")).lower()
            if owner and owner in owner_blocklist:
                continue
            publisher = str(sp.get("publisherName", "")).lower()
            if publisher and publisher in publisher_blocklist:
                continue
            filtered.append(sp)
        return filtered

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
        created_dt = parse_datetime(sp.get("createdDateTime"))
        return EnterpriseAppRecord(
            app_object_id=app_object_id,
            app_id=sp.get("appId"),
            display_name=sp.get("displayName", "<unknown>"),
            account_enabled=bool(sp.get("accountEnabled", True)),
            user_signins_last_30_days=signin_count,
            has_valid_certificate=has_valid_cert,
            nearest_cert_expiry=nearest_expiry,
            sampled_until=sampled_until,
            app_description=sp.get("appDescription"),
            app_owner_organization_id=sp.get("appOwnerOrganizationId"),
            app_role_assignment_required=sp.get("appRoleAssignmentRequired"),
            created_datetime=created_dt,
            description=sp.get("description"),
            homepage=sp.get("homepage"),
            login_url=sp.get("loginUrl"),
            notes=sp.get("notes"),
            notification_emails=sp.get("notificationEmailAddresses"),
            saml_sso_settings=sp.get("samlSingleSignOnSettings"),
            preferred_single_sign_on_mode=sp.get("preferredSingleSignOnMode"),
            tags=sp.get("tags"),
        )

    def _fetch_signin_count(self, app_id: Optional[str]) -> int:
        if not app_id:
            return 0
        start_time = datetime.now(timezone.utc) - timedelta(days=self.config.lookback_days)
        start_iso = start_time.isoformat().replace("+00:00", "Z")
        params = {
            "$filter": f"appId eq '{app_id}' and createdDateTime ge {start_iso}",
            "$top": str(self.config.fetch_page_size),
        }
        headers = {"ConsistencyLevel": "eventual"}
        total = 0
        for _ in self.graph_client.paginate("/auditLogs/signIns", params=params, headers=headers):
            total += 1
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


def _has_hide_tag(sp: Dict) -> bool:
    tags = sp.get("tags", [])
    if not isinstance(tags, list):
        return False
    return any(isinstance(tag, str) and tag.lower() == "hideapp" for tag in tags)
