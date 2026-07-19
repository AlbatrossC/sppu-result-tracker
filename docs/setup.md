# SPPU Result Tracker — Setup

## How it works

```
cron-job.org  --POST-->  Vercel /api/trigger
                              |
                              v
                    GitHub Actions (dispatched)
                              |
                +-------------+-------------+
                v                           v
          Fetch SPPU results          Send Discord notification
                v
          Write to Neon DB
                                            ^
Vercel website reads Neon DB ---------------+
```

## Navigation
1. [Clone & install](#1-clone--install)
2. [Neon database](#2-neon-database)
3. [Discord webhook](#3-discord-webhook)
4. [Workflow secret & GitHub token](#4-workflow-secret--github-token)
5. [Local `.env`](#5-local-env)
6. [Test locally](#6-test-locally)
7. [Push & add GitHub Actions secrets](#7-push--add-github-actions-secrets)
8. [Deploy to Vercel](#8-deploy-to-vercel)
9. [Verify deployment](#9-verify-deployment)
10. [Run workflow manually](#10-run-workflow-manually)
11. [Set up cron-job.org](#11-set-up-cron-joborg)
12. [Env var reference](#env-var-reference)
13. [Troubleshooting](#troubleshooting)

---

## 1. Clone & install

```powershell
git clone https://github.com/AlbatrossC/sppu-result-tracker.git
cd sppu-result-tracker
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```
`.env` holds secrets — never commit it.

## 2. Neon database

1. Create a project at [console.neon.tech](https://console.neon.tech/).
2. Open **SQL Editor**, paste the contents of `src/schema.sql`, run it.
   - Confirm tables `results` and `results_history` were created.
3. Under **Connect / Connection Details**, enable **connection pooling** and copy the pooled connection string:
   ```
   postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require&channel_binding=require
   ```
   → save as **`DATABASE_URL`**.

## 3. Discord webhook

1. In your server, open the target channel → **Integrations → Webhooks → New Webhook**.
2. Copy the webhook URL → save as **`DISCORD_WEBHOOK_URL`**.

## 4. Workflow secret & GitHub token

**`WORKFLOW_SECRET`** — a shared password between cron-job.org and Vercel. Generate one:
```powershell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

**`GH_API_TOKEN`** — lets Vercel trigger your GitHub Actions workflow.
1. [github.com/settings/personal-access-tokens](https://github.com/settings/personal-access-tokens) → **Generate new token** (fine-grained).
2. Repository access: only `AlbatrossC/sppu-result-tracker`.
3. Permissions: `Actions: Read and write`, `Contents: Read-only`, `Metadata: Read-only`.
4. Copy the token → save as **`GH_API_TOKEN`**.

## 5. Local `.env`

```env
DATABASE_URL=...
DISCORD_WEBHOOK_URL=...
WORKFLOW_SECRET=...
GH_API_TOKEN=...
```
Optional (already defaulted in code — only set if you renamed things):
```env
GH_REPO_NAME=AlbatrossC/sppu-result-tracker
GH_WORKFLOW_FILE=fetch.yml
GH_REF_BRANCH=main
```

## 6. Test locally

```powershell
python -m pytest -q
python -m src.actions          # run the scraper once
python app.py                  # run the site at http://127.0.0.1:5000/
```
Check `/api/results` and `/api/health` locally before deploying.

## 7. Push & add GitHub Actions secrets

```powershell
git add .
git commit -m "Simplify tracker setup"
git push origin main
```

Repo → **Settings → Secrets and variables → Actions → New repository secret**. Add only:
- `DATABASE_URL`
- `DISCORD_WEBHOOK_URL`

(`WORKFLOW_SECRET` and `GH_API_TOKEN` are not needed here — those are for Vercel.)

## 8. Deploy to Vercel

1. [vercel.com](https://vercel.com/) → sign in with GitHub → **Add New → Project** → import `AlbatrossC/sppu-result-tracker`.
2. Install command: `pip install -r requirements.txt`. Leave build/output commands as-is.
3. Before deploying, add environment variables (below) for **Production, Preview, and Development**:
   ```env
   DATABASE_URL
   DISCORD_WEBHOOK_URL
   WORKFLOW_SECRET
   GH_API_TOKEN
   ```
4. **Redeploy after any env var change** — existing deployments won't pick up new values automatically.

## 9. Verify deployment

Check these return `HTTP 200`:
```
https://sppu-result-tracker.vercel.app/
https://sppu-result-tracker.vercel.app/api/results   → JSON array
https://sppu-result-tracker.vercel.app/api/health    → JSON health object
```
If `/api/results` returns `503`, see [Troubleshooting](#troubleshooting).

## 10. Run workflow manually

Repo → **Actions → SPPU Fetch Results → Run workflow** (branch `main`).
Expect: a green run, updated `results` table in Neon, and a Discord message only if results changed.

## 11. Set up cron-job.org

Create a job at [console.cron-job.org/jobs/create](https://console.cron-job.org/jobs/create):

| Setting | Value |
|---|---|
| URL | `https://sppu-result-tracker.vercel.app/api/trigger` |
| Method | `POST` |
| Header | `Content-Type: application/json` |
| Body | `{"key":"<your WORKFLOW_SECRET>"}` |
| Schedule | `*/15 * * * *` (UTC) |
| Timeout | 30s |
| Redirects as success | No |

Turn on failure notifications (recommended: notify after 1 failure, notify on recovery, notify before job auto-disables, notify 7 days before TLS expiry).

**Expected response:** `HTTP 200` with `{"message": "Workflow accepted", "timestamp": "..."}`.

---

## Env var reference

| Variable | Local `.env` | GitHub Actions secret | Vercel env var |
|---|:---:|:---:|:---:|
| `DATABASE_URL` | ✅ | ✅ | ✅ |
| `DISCORD_WEBHOOK_URL` | ✅ | ✅ | ✅ |
| `WORKFLOW_SECRET` | ✅ | — | ✅ |
| `GH_API_TOKEN` | ✅ | — | ✅ |

## Troubleshooting

**`/api/results` → 503**
- `DATABASE_URL` missing in Vercel
- Vercel not redeployed after env changes
- `schema.sql` wasn't run in that Neon database
- `DATABASE_URL` has stray quotes/spaces or wrong password

**cron-job.org request fails**
- `401` — body `key` doesn't match `WORKFLOW_SECRET` in Vercel
- `503` — Vercel missing `WORKFLOW_SECRET` or `GH_API_TOKEN`
- `502` — GitHub rejected the workflow dispatch (check `GH_API_TOKEN` permissions/expiry)