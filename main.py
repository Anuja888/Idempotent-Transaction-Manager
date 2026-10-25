"""
Idempotent Transaction Processor — FastAPI application.

Key design decisions
────────────────────
• Every charge carries a client-provided UUID idempotency key.  The key is
  stored with a UNIQUE constraint; any duplicate INSERT raises IntegrityError
  which is caught and the winning transaction's cached response is replayed.
• Account rows are locked with SELECT … FOR UPDATE before any balance mutation
  so concurrent distinct charges serialize correctly.
• All money is stored / computed as Python Decimal / PostgreSQL NUMERIC(12,2).
  Float values only appear in JSON response bodies for human readability.
• The _do_charge() helper is shared between the HTTP endpoint and the
  in-process concurrency-test runners so the business logic is tested once.
"""

import uuid
import asyncio
from decimal import Decimal
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
import logging
import os

from database import get_db, engine, Base, AsyncSessionLocal
from models import Account, Transaction

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transaction_processor")


# ── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create DB tables on startup (idempotent — safe to run repeatedly)."""
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialised.")
    except Exception as exc:          # pragma: no cover
        logger.warning("Could not initialise DB on startup: %s", exc)
    yield
    await engine.dispose()
    logger.info("Database connections closed.")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Idempotent Transaction Processor",
    description=(
        "Payment-style transaction processor demonstrating idempotent charging "
        "with PostgreSQL row-level locking and UNIQUE constraint enforcement."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic schemas ──────────────────────────────────────────────────────────
class ChargeRequest(BaseModel):
    idempotency_key: uuid.UUID
    account_id: str = Field(..., min_length=1, max_length=128)
    amount: Decimal = Field(..., gt=0, decimal_places=2)


class AccountCreateRequest(BaseModel):
    account_id: str = Field(..., min_length=1, max_length=128)
    balance: Decimal = Field(..., ge=0, decimal_places=2)


# ── Core charge logic ─────────────────────────────────────────────────────────
async def _do_charge(
    idempotency_key: uuid.UUID,
    account_id: str,
    amount: Decimal,
    db: AsyncSession,
) -> tuple[int, dict]:
    """
    Execute one idempotent charge inside a single serialised DB transaction.

    Returns (http_status_code, response_body_dict).
    Raises IntegrityError when a concurrent request inserts the same
    idempotency_key first — the caller must catch this and replay the winner.
    """
    async with db.begin():
        # ── 1. Idempotency check ──────────────────────────────────────────
        existing = await db.execute(
            select(Transaction).where(Transaction.idempotency_key == idempotency_key)
        )
        tx = existing.scalar_one_or_none()
        if tx is not None:
            logger.info("Idempotency hit for key %s", idempotency_key)
            return tx.response_code, tx.response_body

        # ── 2. Row-level lock on the account ──────────────────────────────
        account_result = await db.execute(
            select(Account).where(Account.id == account_id).with_for_update()
        )
        account = account_result.scalar_one_or_none()

        # ── 3. Business logic ─────────────────────────────────────────────
        if not account:
            # Cannot insert a Transaction for a non-existent account (FK constraint).
            # Return 404 directly without persisting — idempotency for not-found
            # requests is best-effort; callers should create the account first.
            return status.HTTP_404_NOT_FOUND, {"error": f"Account '{account_id}' not found"}

        elif account.balance >= amount:
            account.balance -= amount
            code = status.HTTP_200_OK
            body = {
                "message": "Charge successful",
                "account_id": account_id,
                "amount": float(amount),
                "new_balance": float(account.balance),
            }
            status_str = "SUCCESS"

        else:
            code = status.HTTP_400_BAD_REQUEST
            body = {
                "error": "Insufficient funds",
                "account_id": account_id,
                "amount": float(amount),
                "balance": float(account.balance),
            }
            status_str = "FAILED_INSUFFICIENT_FUNDS"

        # ── 4. Persist the transaction (cached response) ──────────────────
        db.add(
            Transaction(
                idempotency_key=idempotency_key,
                account_id=account_id,
                amount=amount,
                status=status_str,
                response_code=code,
                response_body=body,
            )
        )

    return code, body


# ── Account endpoints ─────────────────────────────────────────────────────────
@app.get("/healthz", tags=["ops"])
async def health():
    """Liveness check — returns DB connectivity status."""
    try:
        async with engine.connect() as conn:
            await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "db": "disconnected", "error": str(exc)},
        )


@app.post("/accounts", tags=["accounts"])
async def create_account(
    payload: AccountCreateRequest, db: AsyncSession = Depends(get_db)
):
    """
    Create a new account or reset an existing account's balance.

    Concurrent calls for the same account_id are safe: if two requests race on
    creation, the UNIQUE PK constraint raises IntegrityError which is caught and
    treated as "account already exists — update it instead".
    """
    try:
        async with db.begin():
            result = await db.execute(
                select(Account).where(Account.id == payload.account_id).with_for_update()
            )
            account = result.scalar_one_or_none()
            if account:
                account.balance = payload.balance
                msg = "Account updated"
            else:
                account = Account(id=payload.account_id, balance=payload.balance)
                db.add(account)
                msg = "Account created"

    except IntegrityError:
        # Concurrent creation race — update the winner's row
        async with db.begin():
            result = await db.execute(
                select(Account).where(Account.id == payload.account_id).with_for_update()
            )
            account = result.scalar_one_or_none()
            if account:
                account.balance = payload.balance
                msg = "Account updated"
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Concurrent account creation conflict.",
                )

    return {
        "message": msg,
        "account_id": account.id,
        "balance": float(account.balance),
    }


@app.get("/accounts/{account_id}", tags=["accounts"])
async def get_account(account_id: str, db: AsyncSession = Depends(get_db)):
    """Retrieve a single account's current balance."""
    account = await db.get(Account, account_id)
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Account not found"
        )
    return {"account_id": account.id, "balance": float(account.balance)}


# ── Charge endpoint ───────────────────────────────────────────────────────────
@app.post("/charge", tags=["charge"])
async def charge(payload: ChargeRequest, db: AsyncSession = Depends(get_db)):
    """
    Idempotent charge.

    SELECT … FOR UPDATE serialises concurrent distinct charges.
    A UNIQUE constraint on idempotency_key prevents double-charging even when
    two requests pass the initial existence check before either commits.
    """
    try:
        code, body = await _do_charge(
            payload.idempotency_key, payload.account_id, payload.amount, db
        )
        return JSONResponse(status_code=code, content=body)

    except IntegrityError as exc:
        # A concurrent request inserted this key first; replay its result.
        logger.warning(
            "IntegrityError (race) for key %s: %s", payload.idempotency_key, exc
        )
        existing = await db.execute(
            select(Transaction).where(
                Transaction.idempotency_key == payload.idempotency_key
            )
        )
        tx = existing.scalar_one_or_none()
        if tx is not None:
            return JSONResponse(status_code=tx.response_code, content=tx.response_body)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Conflict during transaction processing. Please retry.",
        )


# ── UI / dashboard API ────────────────────────────────────────────────────────
@app.get("/api/accounts", tags=["ui"])
async def list_accounts(db: AsyncSession = Depends(get_db)):
    """List all accounts ordered alphabetically."""
    result = await db.execute(select(Account).order_by(Account.id))
    return [
        {
            "account_id": a.id,
            "balance": float(a.balance),
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in result.scalars().all()
    ]


@app.get("/api/transactions", tags=["ui"])
async def list_transactions(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    account_id: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
):
    """
    Retrieve recent transactions, newest first.

    Supports pagination (limit / offset) and optional filtering by
    account_id and/or status (SUCCESS | FAILED_INSUFFICIENT_FUNDS | FAILED_NOT_FOUND).
    """
    q = select(Transaction).order_by(Transaction.id.desc())
    if account_id:
        q = q.where(Transaction.account_id == account_id)
    if status_filter:
        q = q.where(Transaction.status == status_filter)
    q = q.limit(limit).offset(offset)

    result = await db.execute(q)
    return [
        {
            "id": tx.id,
            "idempotency_key": str(tx.idempotency_key),
            "account_id": tx.account_id,
            "amount": float(tx.amount),
            "status": tx.status,
            "response_code": tx.response_code,
            "response_body": tx.response_body,
            "created_at": tx.created_at.isoformat() if tx.created_at else None,
        }
        for tx in result.scalars().all()
    ]


@app.get("/api/stats", tags=["ui"])
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Aggregate statistics for the dashboard header cards.
    Returns counts and totals without loading all rows.
    """
    from sqlalchemy import func as sqlfunc, case

    # Account stats
    acc_result = await db.execute(
        select(
            sqlfunc.count(Account.id).label("total_accounts"),
            sqlfunc.coalesce(sqlfunc.sum(Account.balance), 0).label("total_balance"),
        )
    )
    acc_row = acc_result.one()

    # Transaction stats
    tx_result = await db.execute(
        select(
            sqlfunc.count(Transaction.id).label("total_tx"),
            sqlfunc.count(
                case((Transaction.status == "SUCCESS", Transaction.id))
            ).label("success_tx"),
            sqlfunc.count(
                case((Transaction.status == "FAILED_INSUFFICIENT_FUNDS", Transaction.id))
            ).label("failed_tx"),
            sqlfunc.coalesce(
                sqlfunc.sum(
                    case((Transaction.status == "SUCCESS", Transaction.amount))
                ),
                0,
            ).label("total_charged"),
        )
    )
    tx_row = tx_result.one()

    # Duplicate requests = total - distinct keys (duplicates hit idempotency cache)
    # A duplicate is when the same key is requested again — we detect them as
    # transactions where the first SELECT finds an existing row.  We can approximate
    # this as: duplicates = rows with status matching an already-recorded key.
    # For simplicity we report duplicate transactions as total charges where
    # the balance was NOT deducted twice (i.e., success_tx only counts once per key).

    return {
        "total_accounts": acc_row.total_accounts,
        "total_balance": float(acc_row.total_balance),
        "total_transactions": tx_row.total_tx,
        "successful_charges": tx_row.success_tx,
        "failed_charges": tx_row.failed_tx,
        "total_charged": float(tx_row.total_charged),
    }


# ── In-process concurrency tests ──────────────────────────────────────────────
async def _run_charges_concurrently(
    account_id: str,
    amount: Decimal,
    keys: list[uuid.UUID],
) -> tuple[int, int]:
    """
    Fire len(keys) charge operations concurrently, each in its own DB session.

    Returns (success_count, fail_count).
    success_count counts results where _do_charge returned HTTP 200 or where
    the IntegrityError fallback found a committed SUCCESS transaction.
    """
    async def one_charge(key: uuid.UUID) -> bool:
        async with AsyncSessionLocal() as db:
            try:
                code, _ = await _do_charge(key, account_id, amount, db)
                return code == status.HTTP_200_OK
            except IntegrityError:
                # Lost the race — look up the winner's result
                async with db.begin():
                    existing = await db.execute(
                        select(Transaction).where(Transaction.idempotency_key == key)
                    )
                    tx = existing.scalar_one_or_none()
                return tx is not None and tx.response_code == status.HTTP_200_OK
            except Exception as exc:
                logger.warning("Unexpected error in concurrent charge for key %s: %s", key, exc)
                return False

    results = await asyncio.gather(*[one_charge(k) for k in keys], return_exceptions=False)
    success = sum(1 for r in results if r)
    return success, len(results) - success


@app.post("/api/run-test-1", tags=["tests"])
async def run_test_1():
    """
    Concurrency Test 1 — 100 simultaneous duplicate requests.

    Seeds a fresh account with $100, fires 100 concurrent charges of $10 all
    sharing the same idempotency key, then asserts the balance was deducted
    exactly once (final balance = $90) and all 100 requests returned HTTP 200.
    """
    account_id = f"ui_dup_{uuid.uuid4().hex[:8]}"
    initial_balance = Decimal("100.00")
    charge_amount = Decimal("10.00")
    idempotency_key = uuid.uuid4()

    async with AsyncSessionLocal() as db:
        async with db.begin():
            db.add(Account(id=account_id, balance=initial_balance))

    success_count, fail_count = await _run_charges_concurrently(
        account_id, charge_amount, [idempotency_key] * 100
    )

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Account).where(Account.id == account_id)
        )
        acc = result.scalar_one_or_none()
        tx_count_result = await db.execute(
            select(Transaction).where(Transaction.account_id == account_id)
        )
        tx_count = len(tx_count_result.scalars().all())

    final_balance = float(acc.balance) if acc else 0.0
    passed = final_balance == 90.0 and success_count == 100 and tx_count == 1

    return {
        "test": "100 duplicate requests",
        "account_id": account_id,
        "initial_balance": 100.0,
        "charge_amount": 10.0,
        "final_balance": final_balance,
        "success_count": success_count,
        "fail_count": fail_count,
        "transaction_records": tx_count,
        "expected_final_balance": 90.0,
        "passed": passed,
        "checks": {
            "balance_deducted_once": final_balance == 90.0,
            "all_returned_success": success_count == 100,
            "single_db_record": tx_count == 1,
        },
    }


@app.post("/api/run-test-2", tags=["tests"])
async def run_test_2():
    """
    Concurrency Test 2 — 50 simultaneous distinct requests.

    Seeds a fresh account with $1,000, fires 50 concurrent $15.50 charges each
    with a unique idempotency key, then asserts all 50 succeeded and the final
    balance is exactly $225.00 (no lost updates).
    """
    account_id = f"ui_diff_{uuid.uuid4().hex[:8]}"
    initial_balance = Decimal("1000.00")
    charge_amount = Decimal("15.50")
    keys = [uuid.uuid4() for _ in range(50)]

    async with AsyncSessionLocal() as db:
        async with db.begin():
            db.add(Account(id=account_id, balance=initial_balance))

    success_count, fail_count = await _run_charges_concurrently(
        account_id, charge_amount, keys
    )

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Account).where(Account.id == account_id)
        )
        acc = result.scalar_one_or_none()
        tx_count_result = await db.execute(
            select(Transaction).where(Transaction.account_id == account_id)
        )
        tx_count = len(tx_count_result.scalars().all())

    final_balance = float(acc.balance) if acc else 0.0
    expected_balance = float(initial_balance - charge_amount * 50)  # 225.00
    passed = (
        abs(final_balance - expected_balance) < 0.01
        and success_count == 50
        and tx_count == 50
    )

    return {
        "test": "50 distinct concurrent requests",
        "account_id": account_id,
        "initial_balance": 1000.0,
        "charge_amount": 15.50,
        "final_balance": final_balance,
        "success_count": success_count,
        "fail_count": fail_count,
        "transaction_records": tx_count,
        "expected_final_balance": expected_balance,
        "passed": passed,
        "checks": {
            "balance_correct": abs(final_balance - expected_balance) < 0.01,
            "all_50_succeeded": success_count == 50,
            "all_50_records": tx_count == 50,
        },
    }


# ── Debug endpoint (safe — no credentials exposed) ────────────────────────────
@app.get("/debug", tags=["ops"])
async def debug_connection():
    """
    Returns DB connectivity status and sanitised config.
    Useful for diagnosing Vercel / Neon connection issues.
    """
    from database import DATABASE_URL, _IS_SERVERLESS, _connect_args
    # Sanitise URL — hide password
    import re
    safe_url = re.sub(r":([^@]+)@", ":***@", DATABASE_URL)

    try:
        async with engine.connect() as conn:
            result = await conn.execute(__import__("sqlalchemy").text("SELECT version()"))
            pg_version = result.scalar()
        db_ok = True
        db_error = None
    except Exception as exc:
        db_ok = False
        pg_version = None
        db_error = str(exc)

    return {
        "db_ok": db_ok,
        "db_url": safe_url,
        "is_serverless": _IS_SERVERLESS,
        "connect_args_keys": list(_connect_args.keys()),
        "pg_version": pg_version,
        "db_error": db_error,
    }


# ── Dashboard ─────────────────────────────────────────────────────────────────
_PUBLIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_index():
    """Serve the single-page dashboard."""
    candidate = os.path.join(_PUBLIC_DIR, "index.html")
    if os.path.isfile(candidate):
        with open(candidate, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(
        content="<h1>Dashboard not found</h1><p>Ensure public/index.html exists.</p>",
        status_code=404,
    )
