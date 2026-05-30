# Interactions live backend

Small read-only API that powers live interaction lookups on the **Interactions** page.
Backed by the dbt staging model `ELISE.DEV_JASONBIAN.STG_INBOUND_INTERACTION_EVENTS`
(falls back to a self-contained dedup over the raw `ELISE.PUBLIC.INBOUND_INTERACTION_EVENTS`).

## Run

```
cd /Users/jason.bian/EliseAI
.venv/bin/python /Users/jason.bian/frontend/backend/server.py
```

Connects via `snowflake/sf_connect.py` (cached SSO token; first run may open a browser).
Serves on `http://localhost:8899`.

## Endpoints

- `GET /api/health` → `{ ok, source }`
- `GET /api/search?q=<term>` → matches `interaction_id` / `id` / `state_id` / `message_id`
  (exact), else `ilike` on `interaction_id` / `message_id` / `sender` / `recipient`.
- `GET /api/trace?interaction_id=<iid>` → full ordered step trace.

## How the page uses it

On load the Interactions page probes `/api/health`. If reachable it switches to
**live** mode (the pill turns green: `live · dbt:stg`) and searches query the API for
any interaction. If not reachable it stays in **cached snapshot** mode and searches the
bundled `traces.json` — which is what the deployed GitHub Pages site uses.
