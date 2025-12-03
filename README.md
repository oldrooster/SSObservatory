# SSObservatory

SSOObservatory inventories Microsoft Entra enterprise applications, captures their recent usage trends, and writes the observations to an external PostgreSQL database. The project ships as a small Python application and can also be executed through the provided Docker image.

## Features
- Connects to Microsoft Graph using an Entra app registration (client credentials flow).
- Enumerates all enterprise applications (service principals) in the tenant.
- Tracks each app's enabled state, certificate health, and how many users have signed in during the last 30 days.
- Creates and continuously upserts into the `enterprise_apps` table inside PostgreSQL.

## Prerequisites
- Python 3.11+ **or** Docker.
- A PostgreSQL database reachable from where you run the app.
- An Entra app registration with the following Microsoft Graph application permissions (granted admin consent): `Application.Read.All`, `AuditLog.Read.All`.

## Configuration
All secrets and connection details are injected via environment variables. Copy `.env.example` to `.env` and fill in the values:

```
cp .env.example .env
```

| Variable | Description |
| --- | --- |
| `AZURE_TENANT_ID` | Directory (tenant) ID hosting the enterprise apps. |
| `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` | Credentials for the Graph app registration. |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE` | PostgreSQL connection details. |
| `LOOKBACK_DAYS` | Window (default 30) for measuring user sign-ins. |
| `GRAPH_PAGE_SIZE` | Page size used when iterating Graph results (1-999). |
| `SERVICE_PRINCIPAL_FILTER` | Microsoft Graph `$filter` expression for selecting enterprise apps (defaults to `servicePrincipalType eq 'Application'`). |
| `EXCLUDE_HIDE_APP_TAG` | `true`/`false`. When true (default) the app drops service principals whose tags include `HideApp`. |
| `EXCLUDE_OWNER_ORGANIZATION_IDS` | Comma-separated list of tenant IDs to ignore (default contains Microsoft's first-party tenant). |
| `EXCLUDE_PUBLISHERS` | Comma-separated list of publisher names to skip locally (default `Microsoft`). |

## Running Locally
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python -m sso_observatory.main
```

## Running with Docker
```
docker build -t ssoobservatory .
docker run --rm --env-file .env ssoobservatory
```

## Data Model
The app automatically creates the following table if it does not exist:

```
CREATE TABLE enterprise_apps (
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
```

Each execution upserts every enterprise app discovered so that the database always reflects the latest snapshot.

## Troubleshooting
- Verify the Entra app registration has access to the `auditLogs/signIns` endpoint; without `AuditLog.Read.All` the sign-in counts will stay at zero.
- If Docker cannot reach PostgreSQL, ensure the container network can resolve and connect to `PGHOST` (consider `host.docker.internal` on macOS/Windows or `--network host` on Linux when appropriate).
