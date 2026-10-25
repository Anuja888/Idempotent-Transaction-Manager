import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:password@localhost:5433/transaction_processor"
)

# Vercel serverless functions are stateless — each invocation may run in a fresh
# process.  NullPool avoids holding open connections between requests, and
# statement_cache_size=0 is required by some managed Postgres proxies
# (Neon, Supabase Supavisor) that don't support the extended query protocol.
_IS_SERVERLESS = os.getenv("VERCEL") is not None or os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

_connect_args: dict = {}
if _IS_SERVERLESS:
    # Many hosted Postgres providers require SSL; asyncpg expects this in connect_args.
    _connect_args["ssl"] = "require"
    _connect_args["statement_cache_size"] = 0

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    # Use NullPool on serverless so connections are never held between invocations.
    **({"poolclass": NullPool} if _IS_SERVERLESS else {"pool_size": 20, "max_overflow": 10}),
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
