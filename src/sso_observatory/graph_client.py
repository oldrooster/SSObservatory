"""Thin wrapper around Microsoft Graph API."""
from __future__ import annotations

import logging
import time
from typing import Dict, Iterator, Optional

import requests
from msal import ConfidentialClientApplication

from .config import AzureConfig

LOGGER = logging.getLogger(__name__)


class GraphClient:
    """Provide authenticated access to Microsoft Graph."""

    def __init__(self, config: AzureConfig, *, max_retries: int = 5, timeout: int = 30) -> None:
        self.config = config
        self.max_retries = max(1, max_retries)
        self.timeout = timeout
        self._app = ConfidentialClientApplication(
            client_id=config.client_id,
            client_credential=config.client_secret,
            authority=f"https://login.microsoftonline.com/{config.tenant_id}",
        )

    def _acquire_token(self) -> str:
        result = self._app.acquire_token_silent(scopes=[self.config.scope], account=None)
        if not result:
            LOGGER.debug("Falling back to client credentials flow for Graph token")
            result = self._app.acquire_token_for_client(scopes=[self.config.scope])
        if "access_token" not in result:
            raise RuntimeError(f"Unable to acquire access token: {result.get('error_description')}")
        return str(result["access_token"])

    def get(
        self,
        resource: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        *,
        absolute: bool = False,
    ) -> Dict:
        token = self._acquire_token()
        url = resource if absolute else f"{self.config.graph_base_url.rstrip('/')}/{resource.lstrip('/')}"
        merged_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if headers:
            merged_headers.update(headers)

        attempt = 0
        while True:
            attempt += 1
            response = requests.get(url, params=params, headers=merged_headers, timeout=self.timeout)
            if response.status_code < 400:
                return response.json()

            will_retry = response.status_code in {429, 503} and attempt < self.max_retries
            if will_retry:
                retry_after = response.headers.get("Retry-After")
                try:
                    wait_seconds = int(retry_after) if retry_after else 5
                except ValueError:
                    wait_seconds = 5
                LOGGER.warning(
                    "Graph throttled (status %s). Retrying in %ss (attempt %s/%s)",
                    response.status_code,
                    wait_seconds,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(wait_seconds)
                continue

            LOGGER.error("Graph request failed: %s", response.text)
            response.raise_for_status()

    def paginate(
        self,
        resource: str,
        params: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Iterator[Dict]:
        next_link: Optional[str] = resource
        first_request = True
        while next_link:
            payload = self.get(
                next_link,
                params=params if first_request else None,
                headers=headers,
                absolute=not first_request,
            )
            for item in payload.get("value", []):
                yield item
            next_link = payload.get("@odata.nextLink")
            first_request = False
