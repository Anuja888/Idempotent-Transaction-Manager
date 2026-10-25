"""
One-time migration script: adds created_at columns and the account_id index
to existing tables without dropping data.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from database import engine, Base


MIGRATION_SQL = """
DO $$
BEGIN
    -- accounts.created_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'accounts' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE accounts ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
        RAISE NOTICE 'Added accounts.created_at';
    ELSE
        RAISE NOTICE 'accounts.created_at already exists';
    END IF;

    -- transactions.created_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'transactions' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE transactions ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();
        RAISE NOTICE 'Added transactions.created_at';
    ELSE
        RAISE NOTICE 'transactions.created_at already exists';
    END IF;

    -- account_id index on transactions
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'transactions' AND indexname = 'ix_transactions_account_id'
    ) THEN
        CREATE INDEX ix_transactions_account_id ON transactions (account_id);
        RAISE NOTICE 'Created ix_transactions_account_id';
    END IF;

END
$$;
"""


async def run():
    async with engine.begin() as conn:
        await conn.execute(text(MIGRATION_SQL))
        # Ensure any new tables/columns defined in models are also created
        from models import Account, Transaction  # noqa: F401
        await conn.run_sync(Base.metadata.create_all)
    print("Migration complete.")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run())
