import os
import ssl
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

# ── Build clean DATABASE_URL ──────────────────────────────────────────────────
# Neon / Supabase connection strings often arrive as:
#   postgresql://user:pass@host/db?sslmode=require&channel_binding=require
# asyncpg needs:
#   postgresql+asyncpg://user:pass@host/db  (SSL via connect_args, not query param)
_raw_url = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:password@localhost:5433/transaction_processor",
)

# Normalise scheme
if _raw_url.startswith("postgres://"):
    _raw_url = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw_url.startswith("postgresql://"):
    _raw_url = _raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)

# Strip query params that asyncpg doesn't understand (sslmode, channel_binding)
# We handle SSL via connect_args instead
if "?" in _raw_url:
    _raw_url = _raw_url.split("?")[0]

DATABASE_URL = _raw_url

# ── Detect serverless environment ─────────────────────────────────────────────
_IS_SERVERLESS = (
    os.getenv("VERCEL") is not None
    or os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None
)

# ── Connection arguments ───────────────────────────────────────────────────────
_connect_args: dict = {}

if _IS_SERVERLESS:
    # Neon / Supabase require SSL. Build an ssl context for asyncpg.
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE   # Neon pooler uses self-signed internally

    _connect_args = {
        "ssl": _ssl_ctx,
        "statement_cache_size": 0,  # Required for Neon / Supabase Supavisor pooler
    }
elif "neon.tech" in DATABASE_URL or "supabase" in DATABASE_URL:
    # Local dev pointing at a hosted DB
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE
    _connect_args = {
        "ssl": _ssl_ctx,
        "statement_cache_size": 0,
    }

# ── Engine ─────────────────────────────────────────────────────────────────────
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    **({"poolclass": NullPool} if _IS_SERVERLESS else {"pool_size": 5, "max_overflow": 5}),
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
