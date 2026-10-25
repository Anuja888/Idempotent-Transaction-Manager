# Idempotent Transaction Manager

A production-quality payment-style transaction processor demonstrating:

- **Idempotent charging** — client-provided UUID keys, UNIQUE constraint enforcement, cached response replay
- **Concurrency-safe balance updates** — PostgreSQL `SELECT … FOR UPDATE` row-level locking
- **Race-condition handling** — `IntegrityError` fallback replays the winning concurrent request
- **Premium fintech dashboard** — dark operator-console UI with live stats, filtering, pagination, and transaction detail drawer

---

## Architecture

```
FastAPI (async) ──► SQLAlchemy asyncpg ──► PostgreSQL 15
      │
      ├── POST /charge          idempotent charge (core logic)
      ├── POST /accounts        create / seed account
      ├── GET  /accounts/{id}   balance lookup
      ├── GET  /api/accounts    list all accounts
      ├── GET  /api/transactions paginated + filtered transaction log
      ├── GET  /api/stats       dashboard aggregate stats
      ├── POST /api/run-test-1  in-process concurrency test: 100 duplicates
      ├── POST /api/run-test-2  in-process concurrency test: 50 distinct
      ├── GET  /healthz         liveness check
      └── GET  /                dashboard SPA
```

### Idempotency mechanism

```
Request arrives with idempotency_key (UUID)
    │
    ▼
SELECT transaction WHERE idempotency_key = ? ──found──► return cached (code, body)
    │ not found
    ▼
SELECT account WHERE id = ? FOR UPDATE  ← serialises concurrent charges
    │
    ├── account missing  ──► 404 (not persisted — no FK target)
    ├── balance < amount ──► 400 + INSERT transaction (cached failure)
    └── balance >= amount──► deduct + 200 + INSERT transaction (cached success)
         │
         └── concurrent race: IntegrityError on UNIQUE(idempotency_key)
                  └──► look up winner's committed row ──► replay response
```

---

## Quick start — Docker Compose (recommended)

```bash
# Start PostgreSQL + app
docker compose up -d

# Dashboard
open http://localhost:8000
```

> Requires Docker Desktop. The `app` service builds from `Dockerfile`.

---

## Local development

### Prerequisites

- Python 3.11+
- PostgreSQL 15 (or Docker for the DB only)

### Setup

```bash
# 1. Clone and enter the project
git clone https://github.com/Anuja888/Idempotent-Transaction-Manager.git
cd "Idempotent-Transaction-Manager"

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS / Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
copy .env.example .env         # Windows
# cp .env.example .env         # macOS / Linux
# Edit .env and set DATABASE_URL

# 5. Start PostgreSQL (Docker)
docker compose up -d db

# 6. Run the application
uvicorn main:app --host 0.0.0.0 --port 8001 --reload

# Dashboard → http://localhost:8001
# API docs  → http://localhost:8001/docs
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:password@localhost:5432/transaction_processor` | PostgreSQL async connection string |
| `TEST_BASE_URL` | `http://127.0.0.1:8001` | Base URL used by pytest (set if server runs on different port) |
| `VERCEL` | — | Set automatically by Vercel to enable serverless pool/SSL settings |

---

## Running tests

The server must be running before running pytest.

```bash
# Start the server (separate terminal)
$env:DATABASE_URL='postgresql+asyncpg://postgres:password@localhost:5433/transaction_processor'
uvicorn main:app --host 0.0.0.0 --port 8001

# Run all tests (new terminal)
$env:TEST_BASE_URL='http://127.0.0.1:8001'
pytest tests/ -v

# Run only unit/integration tests (fast, ~10 s)
pytest tests/ -v --deselect tests/test_api.py::test_concurrent_duplicate_requests \
                 --deselect tests/test_api.py::test_concurrent_distinct_requests \
                 --deselect tests/test_api.py::test_concurrent_duplicates_insufficient_balance

# Run original external concurrency harness
python test_concurrency.py
```

### Test coverage

| Test | What it verifies |
|---|---|
| `test_health` | `/healthz` liveness endpoint |
| `test_create_account` | Account creation |
| `test_update_account_balance` | Balance reset via POST /accounts |
| `test_get_account` | Single account balance lookup |
| `test_get_unknown_account_returns_404` | Missing account 404 |
| `test_list_accounts` | Account list API |
| `test_successful_charge` | Happy-path charge |
| `test_insufficient_funds` | 400 on low balance |
| `test_unknown_account_charge_returns_404` | Charge on missing account |
| `test_charge_exact_balance` | Charge draining exact balance |
| `test_charge_zero_rejected` | Pydantic `gt=0` validation |
| `test_charge_negative_rejected` | Negative amount rejected |
| `test_charge_invalid_uuid_rejected` | Malformed UUID rejected |
| `test_sequential_retry_same_key` | Idempotency: sequential retry |
| `test_retry_preserves_failure_response` | Cached failure replayed on retry |
| `test_retry_not_found_preserved` | Not-found retries stay 404 |
| `test_different_keys_deduct_separately` | Distinct keys = distinct charges |
| `test_transactions_list` | Transaction log API |
| `test_transactions_filter_by_status` | Status filtering |
| `test_transactions_pagination` | limit / offset pagination |
| `test_stats` | Aggregate stats endpoint |
| **`test_concurrent_duplicate_requests`** | **100 simultaneous same-key requests → balance deducted once, all 100 return 200, 1 DB record** |
| **`test_concurrent_distinct_requests`** | **50 simultaneous unique-key requests → all 50 land, final balance = $225.00, no lost updates** |
| `test_concurrent_duplicates_insufficient_balance` | 100 concurrent duplicates on low-balance account → all return 400, balance unchanged |

---

## API reference

### `POST /accounts`
Create or reset an account.
```json
{ "account_id": "alice", "balance": "500.00" }
```

### `GET /accounts/{account_id}`
Returns current balance.

### `POST /charge`
Idempotent charge. Retrying with the same `idempotency_key` returns the cached response without re-charging.
```json
{
  "idempotency_key": "550e8400-e29b-41d4-a716-446655440000",
  "account_id": "alice",
  "amount": "25.00"
}
```

### `GET /api/transactions`
Paginated transaction log. Query params: `limit`, `offset`, `account_id`, `status`.

### `GET /api/stats`
Aggregate dashboard stats.

### `POST /api/run-test-1` / `POST /api/run-test-2`
Trigger in-process concurrency tests from the dashboard.

---

## Database schema

```sql
accounts (
    id         VARCHAR        PRIMARY KEY,
    balance    NUMERIC(12,2)  NOT NULL,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now()
)

transactions (
    id               SERIAL         PRIMARY KEY,
    idempotency_key  UUID           NOT NULL UNIQUE,   -- enforces idempotency at DB level
    account_id       VARCHAR        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount           NUMERIC(12,2)  NOT NULL,
    status           VARCHAR        NOT NULL,           -- SUCCESS | FAILED_INSUFFICIENT_FUNDS
    response_code    INTEGER        NOT NULL,           -- HTTP status cached
    response_body    JSONB          NOT NULL,           -- full response body cached
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
)
```

To apply schema to a hosted database (Neon, Supabase, Railway):
```bash
psql $DATABASE_URL -f schema.sql
```

---

## Vercel deployment

### Prerequisites
A hosted PostgreSQL database. Free options:
- **[Neon](https://neon.tech)** — serverless Postgres, free tier, easiest setup
- **[Supabase](https://supabase.com)** — free tier, standard Postgres
- **[Railway](https://railway.app)** — free trial, straightforward

### Steps

1. **Import the repo** on [vercel.com/new](https://vercel.com/new) → GitHub → `Anuja888/Idempotent-Transaction-Manager`
   - Framework Preset: **Other** (auto-detected as Python)
   - Root Directory: leave as `/`

2. **Set environment variable** in Vercel → Settings → Environment Variables:
   ```
   DATABASE_URL = postgresql+asyncpg://user:pass@host/dbname
   ```
   > For Neon: use the pooled connection string, replace `postgresql://` with `postgresql+asyncpg://`  
   > The app automatically enables SSL + `statement_cache_size=0` for hosted providers.

3. **Run the schema once** against your hosted DB (Neon/Supabase both have a SQL editor):
   ```sql
   -- paste contents of schema.sql
   ```

4. **Deploy** — Vercel will:
   - Detect `fastapi` in `requirements.txt` → Python 3.12 runtime
   - Install only `requirements.txt` (test deps are in `requirements-dev.txt`, excluded)
   - Route all traffic via `vercel.json` rewrite → `api/index.py` → FastAPI `app`

### How the Vercel config works

```
vercel.json
  functions.api/index.py → Python 3.12 serverless function
  rewrites /(.*) → /api/index  (every request hits FastAPI)

api/index.py
  sys.path.insert(project_root)
  from main import app   ← Vercel detects this ASGI app

database.py
  VERCEL=1 (set in vercel.json env) →
    NullPool          (no persistent connections between invocations)
    ssl="require"     (required by all hosted Postgres providers)
    statement_cache_size=0  (Neon/Supabase Supavisor compatible)
```

### Notes
- Dashboard (`/`) is served by FastAPI reading `public/index.html` — the catch-all rewrite means no static files bypass the function
- `asyncpg` compiles against the Vercel build environment automatically  
- Cold starts on free tier: ~1–2 s; subsequent requests are fast

---

## Verified test results

```
pytest tests/ — 24 tests

Unit / integration (21):   21 passed  ✓
Concurrency Test 1 (100 duplicate requests):
  Fired=100, HTTP 200=100, Failed=0
  Balance: $100.00 → $90.00  ✓ deducted exactly once
  DB records for key: 1      ✓
Concurrency Test 2 (50 distinct requests):
  Fired=50, HTTP 200=50, Failed=0
  Balance: $1,000.00 → $225.00  ✓ no lost updates
  DB records: 50             ✓
Concurrent duplicates insufficient balance:
  All 100 returned 400, balance unchanged  ✓

test_concurrency.py (external HTTP harness):
  Test 1: PASSED  Test 2: PASSED
```
