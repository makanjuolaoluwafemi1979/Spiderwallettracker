# SpiderWalletBot — Render Deployment

## What changed from the local/ngrok version
- All secrets (`TELEGRAM_TOKEN`, `CHAT_ID`, `HELIUS_API_KEY`, `WEBHOOK_SECRET`) are now
  **required environment variables** — there are no hardcoded fallback values in the
  code anymore. The app raises a clear error and refuses to start if any are missing.
- `APP_URL` auto-detects Render's injected `RENDER_EXTERNAL_URL`, so you don't need to
  set it manually on Render (still supported for local ngrok use).
- Startup (font/template caching, webhook registration, scheduler) now runs at
  **module import time**, not just inside `if __name__ == "__main__"`. This matters
  because Render/gunicorn *imports* the app rather than running it as a script —
  the old structure would have silently skipped webhook registration in production.
- Added `requirements.txt`, `render.yaml`, `Procfile`, `.env.example`.

## ⚠️ Rotate your credentials
The token/chat ID/API key that were hardcoded as defaults in the previous version of
this file were **real, live credentials sitting in plaintext**. Before deploying this
version, rotate all of them:
- Telegram: talk to @BotFather → `/revoke` (or just generate a new bot / new token)
- Helius: dashboard → regenerate API key
- Pick a new random `WEBHOOK_SECRET`

## One-time setup
1. Push this code to a GitHub repo (private is fine).
2. In Render: **New → Blueprint**, point it at the repo. Render will read `render.yaml`
   automatically.
3. In the service's **Environment** tab, set the required variables (see
   `.env.example` for the full list). Leave `APP_URL` unset — Render handles it.
4. Deploy. Watch the logs for the `🕷 SpiderWalletBot Started` Telegram message with
   the full stats breakdown — that confirms webhook registration succeeded.

## Persistent storage
`wallet_intel.db` (the confidence/lifecycle/discovery database) is written to
`/var/data`, which `render.yaml` mounts as a 1GB persistent disk. Without this,
Render's filesystem is ephemeral and you'd lose all wallet scoring history on every
deploy or restart.

## Why `--workers 1`
Watched-wallet lists, the TTL/dedup caches, and the APScheduler background jobs all
live in one process's memory. Multiple gunicorn workers would each get their own copy
— duplicate alerts, inconsistent dedup, the scheduler firing once per worker. Use
`--threads` for concurrency instead (already set to 8, matching the app's internal
thread pool). Don't change this unless you also move shared state to something
external like Redis.

## Health check
Render pings `/status` to know the service is alive — this route already exists in
the bot and returns basic uptime/config info.

## Local development
Copy `.env.example` to `.env`, fill in real values, `pip install -r requirements.txt`,
then `python SpiderWalletBot2.py`. Use `NGROK_SUBDOMAIN` (or `APP_URL`) to get a public
URL for Helius to POST to, same as before.
