-- Idempotent Transaction Processor — canonical schema.
-- Run this once against a fresh hosted PostgreSQL (Neon / Supabase / Railway etc.)
-- The FastAPI lifespan event runs CREATE TABLE IF NOT EXISTS on every cold start
-- via SQLAlchemy, so this file is optional but useful for manual setup / auditing.

CREATE TABLE IF NOT EXISTS accounts (
    id         VARCHAR        PRIMARY KEY,
    balance    NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id               SERIAL         PRIMARY KEY,
    idempotency_key  UUID           NOT NULL UNIQUE,
    account_id       VARCHAR        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount           NUMERIC(12, 2) NOT NULL,
    status           VARCHAR        NOT NULL,    -- SUCCESS | FAILED_INSUFFICIENT_FUNDS | FAILED_NOT_FOUND
    response_code    INTEGER        NOT NULL,
    response_body    JSONB          NOT NULL,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_accounts_id
    ON accounts (id);

CREATE INDEX IF NOT EXISTS ix_transactions_account_id
    ON transactions (account_id);

CREATE INDEX IF NOT EXISTS ix_transactions_created_at
    ON transactions (created_at DESC);

-- ── Migration guard: add created_at to existing tables without dropping data ──
DO $$
BEGIN
    -- accounts.created_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'accounts' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE accounts ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
    END IF;

    -- transactions.created_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'transactions' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE transactions ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
    END IF;

    -- transactions.account_id index
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'transactions' AND indexname = 'ix_transactions_account_id'
    ) THEN
        CREATE INDEX ix_transactions_account_id ON transactions (account_id);
    END IF;
END
$$;
