# SPPU Result Tracker

Tracker for the SPPU result dashboard.

The app keeps only two database tables:

- `results`: current mirror of the SPPU result page.
- `results_history`: every added, updated, or removed result event.

## 1. Create the database

Open the Neon SQL Editor and run `src/schema.sql`.

Use the pooled Neon PostgreSQL URI for deployed code:

```text
postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require&channel_binding=require
```

The schema drops the old operational tables:

```text
tracker_runs
tracker_state
pending_changes
notification_outbox
```

## 2. Configure GitHub

Add these repository Actions secrets:

- `DATABASE_URL`
- `DISCORD_WEBHOOK_URL`

The workflow intentionally has only `workflow_dispatch`; it has no GitHub cron.

## 3. Configure Vercel

Add these environment variables to every environment that serves `app.py`:

- `DATABASE_URL`
- `WORKFLOW_SECRET`: a long random value used only by the external cron request.
- `GH_API_TOKEN`: a fine-grained GitHub token with Actions write access to this repository.

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

## Scenarios

First run:

```text
SPPU has 100 results and the database is empty.
results gets 100 rows.
results_history gets no rows.
Discord gets no notifications.
```

New result:

```text
SPPU adds "BCA" dated 19-Jul-2026.
results inserts that row with notification_sent = false.
results_history inserts change_type = added.
Discord sends the message, then both flags become true.
```

Updated result:

```text
Database has "Engineering" dated 18-Jul-2026.
SPPU now shows "Engineering" dated 19-Jul-2026.
results updates that row to 19-Jul-2026 with notification_sent = false.
results_history records old_result_date and new_result_date.
```

Removed result:

```text
Database has "Pharmacy" dated 18-Jul-2026.
SPPU no longer shows it.
results deletes that row.
results_history records change_type = removed.
```

Discord failure:

```text
A history row stays notification_sent = false and stores notification_error.
The next successful run retries unsent history notifications.
```

## Local Checks

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
python -m src.actions
```

`python -m src.actions` requires the production environment variables and a
database initialized with `src/schema.sql`.
