"""PostgreSQL helpers for persisting observations."""
from __future__ import annotations

import logging
from typing import Sequence

import psycopg2
from psycopg2.extras import execute_values

from .config import DatabaseConfig
from .data_collector import EnterpriseAppRecord

LOGGER = logging.getLogger(__name__)


class DatabaseClient:
    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        conn_params = dict(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
        )
        if config.sslmode:
            conn_params["sslmode"] = config.sslmode
        self.conn = psycopg2.connect(**conn_params)
        self.conn.autocommit = True
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        LOGGER.debug("Ensuring enterprise_apps table exists")
        create_sql = """
        CREATE TABLE IF NOT EXISTS enterprise_apps (
            app_object_id TEXT PRIMARY KEY,
            app_id TEXT,
            display_name TEXT,
            account_enabled BOOLEAN,
            user_signins_last_30_days INTEGER,
            has_valid_certificate BOOLEAN,
            nearest_cert_expiry TIMESTAMPTZ,
            sampled_until TIMESTAMPTZ NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_sql)

    def upsert_apps(self, rows: Sequence[EnterpriseAppRecord]) -> None:
        if not rows:
            return
        insert_sql = """
        INSERT INTO enterprise_apps (
            app_object_id,
            app_id,
            display_name,
            account_enabled,
            user_signins_last_30_days,
            has_valid_certificate,
            nearest_cert_expiry,
            sampled_until
        ) VALUES %s
        ON CONFLICT (app_object_id) DO UPDATE SET
            app_id = EXCLUDED.app_id,
            display_name = EXCLUDED.display_name,
            account_enabled = EXCLUDED.account_enabled,
            user_signins_last_30_days = EXCLUDED.user_signins_last_30_days,
            has_valid_certificate = EXCLUDED.has_valid_certificate,
            nearest_cert_expiry = EXCLUDED.nearest_cert_expiry,
            sampled_until = EXCLUDED.sampled_until,
            synced_at = NOW();
        """
        values = [
            (
                row.app_object_id,
                row.app_id,
                row.display_name,
                row.account_enabled,
                row.user_signins_last_30_days,
                row.has_valid_certificate,
                row.nearest_cert_expiry,
                row.sampled_until,
            )
            for row in rows
        ]
        with self.conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        LOGGER.debug("Upserted %s enterprise apps", len(rows))

    def close(self) -> None:
        self.conn.close()
