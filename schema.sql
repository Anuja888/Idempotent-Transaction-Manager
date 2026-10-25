-- Idempotent Transaction Processor schema.
-- Run this once against your hosted PostgreSQL (Neon / Supabase / etc.) to make
-- sure the tables exist independently of the app's startup. The FastAPI lifespan
-- also runs CREATE TABLE IF NOT EXISTS on cold start, so this is optional but
-- handy as a one-time setup / verification step.

CREATE TABLE IF NOT EXISTS accounts (
    id       VARCHAR        PRIMARY KEY,
    balance  NUMERIC(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id               SERIAL         PRIMARY KEY,
    idempotency_key  UUID           NOT NULL UNIQUE,
    account_id       VARCHAR        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount           NUMERIC(12, 2) NOT NULL,
    status           VARCHAR        NOT NULL,
    response_code    INTEGER        NOT NULL,
    response_body    JSONB          NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_accounts_id ON accounts (id);
