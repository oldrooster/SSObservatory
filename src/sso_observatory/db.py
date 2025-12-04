"""PostgreSQL helpers for persisting observations."""
from __future__ import annotations

import logging
from typing import Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

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
            app_description TEXT,
            app_owner_organization_id TEXT,
            app_role_assignment_required BOOLEAN,
            created_datetime TIMESTAMPTZ,
            description TEXT,
            homepage TEXT,
            login_url TEXT,
            notes TEXT,
            notification_emails JSONB,
            saml_sso_settings JSONB,
            preferred_single_sign_on_mode TEXT,
            tags JSONB,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_sql)
            migrations = [
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS app_description TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS app_owner_organization_id TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS app_role_assignment_required BOOLEAN",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS created_datetime TIMESTAMPTZ",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS description TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS homepage TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS login_url TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS notes TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS notification_emails JSONB",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS saml_sso_settings JSONB",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS preferred_single_sign_on_mode TEXT",
                "ALTER TABLE enterprise_apps ADD COLUMN IF NOT EXISTS tags JSONB",
            ]
            for statement in migrations:
                cur.execute(statement)

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
            sampled_until,
            app_description,
            app_owner_organization_id,
            app_role_assignment_required,
            created_datetime,
            description,
            homepage,
            login_url,
            notes,
            notification_emails,
            saml_sso_settings,
            preferred_single_sign_on_mode,
            tags
        ) VALUES %s
        ON CONFLICT (app_object_id) DO UPDATE SET
            app_id = EXCLUDED.app_id,
            display_name = EXCLUDED.display_name,
            account_enabled = EXCLUDED.account_enabled,
            user_signins_last_30_days = EXCLUDED.user_signins_last_30_days,
            has_valid_certificate = EXCLUDED.has_valid_certificate,
            nearest_cert_expiry = EXCLUDED.nearest_cert_expiry,
            sampled_until = EXCLUDED.sampled_until,
            app_description = EXCLUDED.app_description,
            app_owner_organization_id = EXCLUDED.app_owner_organization_id,
            app_role_assignment_required = EXCLUDED.app_role_assignment_required,
            created_datetime = EXCLUDED.created_datetime,
            description = EXCLUDED.description,
            homepage = EXCLUDED.homepage,
            login_url = EXCLUDED.login_url,
            notes = EXCLUDED.notes,
            notification_emails = EXCLUDED.notification_emails,
            saml_sso_settings = EXCLUDED.saml_sso_settings,
            preferred_single_sign_on_mode = EXCLUDED.preferred_single_sign_on_mode,
            tags = EXCLUDED.tags,
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
                row.app_description,
                row.app_owner_organization_id,
                row.app_role_assignment_required,
                row.created_datetime,
                row.description,
                row.homepage,
                row.login_url,
                row.notes,
                Json(row.notification_emails) if row.notification_emails is not None else None,
                Json(row.saml_sso_settings) if row.saml_sso_settings is not None else None,
                row.preferred_single_sign_on_mode,
                Json(row.tags) if row.tags is not None else None,
            )
            for row in rows
        ]
        with self.conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        LOGGER.debug("Upserted %s enterprise apps", len(rows))

    def close(self) -> None:
        self.conn.close()
