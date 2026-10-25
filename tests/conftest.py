"""
pytest fixtures for the Idempotent Transaction Processor test suite.

Requires the server to already be running (TEST_BASE_URL env var,
defaults to http://127.0.0.1:8001).

All tests create isolated accounts with unique IDs, so the suite is
repeatable without wiping the database between runs.
"""

import os
import uuid
from decimal import Decimal

import pytest_asyncio
import httpx

BASE_URL = os.getenv("TEST_BASE_URL", "http://127.0.0.1:8001")


@pytest_asyncio.fixture
async def client():
    """
    Per-test AsyncClient.  Using function scope avoids event-loop teardown
    issues with pytest-asyncio on Python 3.13+.
    """
    limits = httpx.Limits(max_keepalive_connections=200, max_connections=200)
    async with httpx.AsyncClient(base_url=BASE_URL, limits=limits, timeout=30.0) as c:
        yield c


async def seed_account(
    client: httpx.AsyncClient,
    balance: Decimal = Decimal("500.00"),
    prefix: str = "test",
) -> str:
    """Create a uniquely-named account and return its ID."""
    account_id = f"{prefix}_{uuid.uuid4().hex[:10]}"
    resp = await client.post(
        "/accounts",
        json={"account_id": account_id, "balance": str(balance)},
    )
    assert resp.status_code == 200, f"seed_account failed: {resp.text}"
    return account_id


@pytest_asyncio.fixture
async def account(client):
    """A fresh $500 account for each test that requests it."""
    return await seed_account(client, Decimal("500.00"))
