# SPPU Result Tracker

The tracker runs only in GitHub Actions. An external cron service calls the
deployed Flask/Vercel endpoint every ten minutes, and that endpoint dispatches
the manual GitHub workflow.

## 1. Create the database

Open the Neon SQL Editor and run `src/schema.sql` once. Use the pooled Neon
PostgreSQL URI for deployed code. It should look like:

```text
postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require&channel_binding=require
```

## 2. Configure GitHub

Add these repository Actions secrets:

- `DATABASE_URL`
- `DISCORD_WEBHOOK_URL`

`DATABASE_URL` must be a full PostgreSQL connection string, including the
database password.

The workflow intentionally has only `workflow_dispatch`; it has no GitHub cron.

## 3. Configure Vercel

Add these environment variables to every environment that serves `app.py`:

- `DATABASE_URL`
- `WORKFLOW_SECRET`: a long random value used only by the external cron request
- `GH_API_TOKEN`: a fine-grained GitHub token with Actions write access to this repository

Optional deployment variables are `GH_REPO_NAME`, `GH_WORKFLOW_FILE`, and
`GH_REF_BRANCH`. Their defaults target `AlbatrossC/sppu-result-tracker`,
`fetch.yml`, and `main`.

## 4. Configure the external cron

Create a job that runs every ten minutes and sends:

```http
POST https://YOUR-VERCEL-DOMAIN/api/trigger
Content-Type: application/json

{"key":"YOUR_WORKFLOW_SECRET"}
```

A `200` response means GitHub accepted the dispatch. A `401` means the cron
secret is wrong, and a `502` or `503` should be retried by the cron provider.

## 5. First run

Trigger `SPPU Fetch Results` manually once before enabling the external cron.
The first successful run silently stores the existing SPPU results as the
baseline. New results notify immediately; updates and removals require two
consecutive valid observations.

## Local checks

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m src.actions
python -m src.debug_sync
```

The last two commands require the production environment variables and a
database initialized with `src/schema.sql`.
