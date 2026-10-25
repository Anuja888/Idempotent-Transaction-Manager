"""
Unit / integration tests for the Idempotent Transaction Processor HTTP API.

These tests run against the live server (started separately) and cover:
  • Account creation and retrieval
  • Single charges — success, insufficient funds, unknown account
  • Idempotency: sequential retry with the same key
  • Concurrency Test 1: 100 simultaneous duplicate requests
  • Concurrency Test 2: 50 simultaneous distinct requests
  • Insufficient-balance concurrent scenario
  • Input validation
  • Health-check endpoint
"""

import asyncio
import uuid
from decimal import Decimal

import pytest
import httpx

from tests.conftest import seed_account, BASE_URL


# ── Helpers ───────────────────────────────────────────────────────────────────
async def charge(
    client: httpx.AsyncClient,
    account_id: str,
    amount: str,
    key: str | None = None,
) -> httpx.Response:
    return await client.post(
        "/charge",
        json={
            "idempotency_key": key or str(uuid.uuid4()),
            "account_id": account_id,
            "amount": amount,
        },
    )


# ═════════════════════════════════════════════════════════════════════════════
# Health
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ═════════════════════════════════════════════════════════════════════════════
# Account management
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_create_account(client):
    account_id = f"create_test_{uuid.uuid4().hex[:8]}"
    resp = await client.post(
        "/accounts", json={"account_id": account_id, "balance": "250.00"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["account_id"] == account_id
    assert data["balance"] == 250.00
    assert data["message"] == "Account created"


@pytest.mark.asyncio
async def test_update_account_balance(client):
    account_id = await seed_account(client, Decimal("100.00"), "update")
    resp = await client.post(
        "/accounts", json={"account_id": account_id, "balance": "999.99"}
    )
    assert resp.status_code == 200
    assert resp.json()["message"] == "Account updated"
    assert resp.json()["balance"] == 999.99


@pytest.mark.asyncio
async def test_get_account(client, account):
    resp = await client.get(f"/accounts/{account}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["account_id"] == account
    assert data["balance"] == 500.00


@pytest.mark.asyncio
async def test_get_unknown_account_returns_404(client):
    resp = await client.get("/accounts/does_not_exist_xyz")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_accounts(client, account):
    resp = await client.get("/api/accounts")
    assert resp.status_code == 200
    ids = [a["account_id"] for a in resp.json()]
    assert account in ids


# ═════════════════════════════════════════════════════════════════════════════
# Single charge scenarios
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_successful_charge(client, account):
    resp = await charge(client, account, "50.00")
    assert resp.status_code == 200
    data = resp.json()
    assert data["message"] == "Charge successful"
    assert data["amount"] == 50.00
    assert data["new_balance"] == 450.00


@pytest.mark.asyncio
async def test_insufficient_funds(client, account):
    resp = await charge(client, account, "999.00")
    assert resp.status_code == 400
    assert "Insufficient funds" in resp.json()["error"]


@pytest.mark.asyncio
async def test_unknown_account_charge_returns_404(client):
    resp = await charge(client, "ghost_account_xyz", "10.00")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_charge_exact_balance(client):
    """Charge exactly the full balance — should succeed leaving $0.00."""
    account_id = await seed_account(client, Decimal("75.00"), "exact")
    resp = await charge(client, account_id, "75.00")
    assert resp.status_code == 200
    assert resp.json()["new_balance"] == 0.00


@pytest.mark.asyncio
async def test_charge_zero_rejected(client, account):
    """Amount must be > 0 — Pydantic validation."""
    resp = await client.post(
        "/charge",
        json={"idempotency_key": str(uuid.uuid4()), "account_id": account, "amount": "0"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_charge_negative_rejected(client, account):
    resp = await client.post(
        "/charge",
        json={"idempotency_key": str(uuid.uuid4()), "account_id": account, "amount": "-5.00"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_charge_invalid_uuid_rejected(client, account):
    resp = await client.post(
        "/charge",
        json={"idempotency_key": "not-a-uuid", "account_id": account, "amount": "10.00"},
    )
    assert resp.status_code == 422


# ═════════════════════════════════════════════════════════════════════════════
# Idempotency — sequential retries
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_sequential_retry_same_key(client, account):
    """Sending the same key twice must deduct the balance only once."""
    key = str(uuid.uuid4())
    r1 = await charge(client, account, "25.00", key)
    r2 = await charge(client, account, "25.00", key)

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Both responses must be identical
    assert r1.json() == r2.json()

    # Balance should only have been reduced once
    balance_resp = await client.get(f"/accounts/{account}")
    assert balance_resp.json()["balance"] == 475.00


@pytest.mark.asyncio
async def test_retry_preserves_failure_response(client, account):
    """
    If the first attempt fails (e.g. insufficient funds), retrying with the
    same key must return the same failure — not succeed later.
    """
    key = str(uuid.uuid4())
    r1 = await charge(client, account, "9999.00", key)
    r2 = await charge(client, account, "9999.00", key)

    assert r1.status_code == 400
    assert r2.status_code == 400
    assert r1.json() == r2.json()


@pytest.mark.asyncio
async def test_retry_not_found_preserved(client):
    """
    Charging a non-existent account returns 404.
    We don't persist a transaction record for unknown accounts (FK constraint),
    so each retry independently returns 404 — consistent behavior is preserved.
    """
    key = str(uuid.uuid4())
    r1 = await charge(client, "ghost_retry_xyz", "10.00", key)
    r2 = await charge(client, "ghost_retry_xyz", "10.00", key)

    assert r1.status_code == 404
    assert r2.status_code == 404
    # Both must carry the same error structure
    assert r1.json().get("error") is not None
    assert r2.json().get("error") is not None


@pytest.mark.asyncio
async def test_different_keys_deduct_separately(client, account):
    """Each unique key is a fresh charge."""
    k1, k2 = str(uuid.uuid4()), str(uuid.uuid4())
    r1 = await charge(client, account, "10.00", k1)
    r2 = await charge(client, account, "10.00", k2)
    assert r1.status_code == 200
    assert r2.status_code == 200

    balance_resp = await client.get(f"/accounts/{account}")
    assert balance_resp.json()["balance"] == 480.00  # 500 - 10 - 10


# ═════════════════════════════════════════════════════════════════════════════
# Concurrency Test 1 — 100 simultaneous duplicate requests
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_concurrent_duplicate_requests():
    """
    100 concurrent requests with the SAME idempotency key.

    Assertions
    ──────────
    • Final balance = $90.00  (charged exactly once)
    • All 100 HTTP responses return 200
    • All 100 response bodies are identical
    • Only one transaction record exists in the database for this key
    """
    limits = httpx.Limits(max_keepalive_connections=150, max_connections=150)
    async with httpx.AsyncClient(
        base_url=BASE_URL, limits=limits, timeout=30.0
    ) as client:
        account_id = await seed_account(client, Decimal("100.00"), "dup")
        key = str(uuid.uuid4())

        tasks = [
            client.post(
                "/charge",
                json={"idempotency_key": key, "account_id": account_id, "amount": "10.00"},
            )
            for _ in range(100)
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # ── Classify responses ────────────────────────────────────────────────
    status_codes = []
    bodies = []
    for r in responses:
        assert not isinstance(r, Exception), f"Request raised exception: {r}"
        status_codes.append(r.status_code)
        bodies.append(r.json())

    # ── Balance deducted exactly once ─────────────────────────────────────
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        bal_resp = await c.get(f"/accounts/{account_id}")
    final_balance = Decimal(str(bal_resp.json()["balance"]))
    assert final_balance == Decimal("90.00"), (
        f"Expected $90.00 but got ${final_balance} — "
        "charge was applied more/fewer than once."
    )

    # ── All 100 returned 200 ──────────────────────────────────────────────
    non_200 = [c for c in status_codes if c != 200]
    assert not non_200, f"{len(non_200)} requests returned non-200 status: {set(non_200)}"

    # ── All response bodies identical ─────────────────────────────────────
    first_body = bodies[0]
    mismatches = [b for b in bodies if b != first_body]
    assert not mismatches, (
        f"{len(mismatches)} responses had different bodies from the first response"
    )

    # ── Only one DB record for the key ────────────────────────────────────
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        tx_resp = await c.get(
            "/api/transactions",
            params={"account_id": account_id, "limit": 10},
        )
    txs = tx_resp.json()
    assert len(txs) == 1, (
        f"Expected 1 transaction record but found {len(txs)}"
    )
    assert txs[0]["status"] == "SUCCESS"


# ═════════════════════════════════════════════════════════════════════════════
# Concurrency Test 2 — 50 simultaneous distinct requests
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_concurrent_distinct_requests():
    """
    50 concurrent requests each with a UNIQUE idempotency key.

    Assertions
    ──────────
    • All 50 return HTTP 200
    • Final balance = $225.00  (1000 − 50 × 15.50, no lost updates)
    • Exactly 50 transaction records exist for the account
    """
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=100)
    async with httpx.AsyncClient(
        base_url=BASE_URL, limits=limits, timeout=30.0
    ) as client:
        account_id = await seed_account(client, Decimal("1000.00"), "dist")

        tasks = [
            client.post(
                "/charge",
                json={
                    "idempotency_key": str(uuid.uuid4()),
                    "account_id": account_id,
                    "amount": "15.50",
                },
            )
            for _ in range(50)
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    # ── All 50 must succeed ───────────────────────────────────────────────
    for r in responses:
        assert not isinstance(r, Exception), f"Request raised exception: {r}"
        assert r.status_code == 200, (
            f"Expected 200 but got {r.status_code}: {r.text}"
        )

    # ── No lost updates ───────────────────────────────────────────────────
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        bal_resp = await c.get(f"/accounts/{account_id}")
    final_balance = Decimal(str(bal_resp.json()["balance"]))
    expected = Decimal("225.00")  # 1000 - 50 * 15.50
    assert final_balance == expected, (
        f"Expected ${expected} but got ${final_balance} — lost updates detected."
    )

    # ── 50 transaction records ────────────────────────────────────────────
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        tx_resp = await c.get(
            "/api/transactions",
            params={"account_id": account_id, "limit": 100},
        )
    txs = tx_resp.json()
    assert len(txs) == 50, f"Expected 50 transaction records, found {len(txs)}"
    statuses = {t["status"] for t in txs}
    assert statuses == {"SUCCESS"}, f"Unexpected statuses: {statuses}"


# ═════════════════════════════════════════════════════════════════════════════
# Concurrent duplicates — insufficient balance scenario
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_concurrent_duplicates_insufficient_balance():
    """
    100 concurrent duplicate requests where the balance is too low.
    All must return the same 400 response and balance must be unchanged.
    """
    limits = httpx.Limits(max_keepalive_connections=150, max_connections=150)
    async with httpx.AsyncClient(
        base_url=BASE_URL, limits=limits, timeout=30.0
    ) as client:
        account_id = await seed_account(client, Decimal("5.00"), "insuf")
        key = str(uuid.uuid4())

        tasks = [
            client.post(
                "/charge",
                json={"idempotency_key": key, "account_id": account_id, "amount": "100.00"},
            )
            for _ in range(100)
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    status_codes = []
    bodies = []
    for r in responses:
        assert not isinstance(r, Exception), f"Request raised: {r}"
        status_codes.append(r.status_code)
        bodies.append(r.json())

    assert all(c == 400 for c in status_codes), (
        f"Expected all 400, got: {set(status_codes)}"
    )
    first = bodies[0]
    assert all(b == first for b in bodies), "Not all bodies matched"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        bal_resp = await c.get(f"/accounts/{account_id}")
    assert bal_resp.json()["balance"] == 5.00  # unchanged


# ═════════════════════════════════════════════════════════════════════════════
# Transaction listing / filtering
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_transactions_list(client, account):
    """Transactions endpoint returns correct data."""
    key = str(uuid.uuid4())
    await charge(client, account, "10.00", key)

    resp = await client.get("/api/transactions", params={"account_id": account})
    assert resp.status_code == 200
    txs = resp.json()
    assert len(txs) >= 1
    found = [t for t in txs if t["idempotency_key"] == key]
    assert len(found) == 1
    tx = found[0]
    assert tx["status"] == "SUCCESS"
    assert tx["response_code"] == 200
    assert tx["amount"] == 10.00
    assert tx["created_at"] is not None


@pytest.mark.asyncio
async def test_transactions_filter_by_status(client, account):
    key = str(uuid.uuid4())
    await charge(client, account, "99999.00", key)  # will fail

    resp = await client.get(
        "/api/transactions",
        params={"account_id": account, "status": "FAILED_INSUFFICIENT_FUNDS"},
    )
    assert resp.status_code == 200
    txs = resp.json()
    assert all(t["status"] == "FAILED_INSUFFICIENT_FUNDS" for t in txs)


@pytest.mark.asyncio
async def test_transactions_pagination(client):
    """limit and offset parameters work correctly."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as c:
        account_id = await seed_account(c, Decimal("500.00"), "page")
        # Create 5 distinct charges
        for _ in range(5):
            await charge(c, account_id, "1.00")

        page1 = await c.get(
            "/api/transactions", params={"account_id": account_id, "limit": 3, "offset": 0}
        )
        page2 = await c.get(
            "/api/transactions", params={"account_id": account_id, "limit": 3, "offset": 3}
        )

    assert len(page1.json()) == 3
    assert len(page2.json()) >= 2  # at least 2 remaining
    # IDs must not overlap between pages
    ids1 = {t["id"] for t in page1.json()}
    ids2 = {t["id"] for t in page2.json()}
    assert not ids1 & ids2


# ═════════════════════════════════════════════════════════════════════════════
# Stats endpoint
# ═════════════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_stats(client, account):
    resp = await client.get("/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_accounts" in data
    assert "total_transactions" in data
    assert "successful_charges" in data
    assert "failed_charges" in data
    assert "total_charged" in data
