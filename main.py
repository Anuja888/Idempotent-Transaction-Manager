import uuid
import asyncio
from decimal import Decimal
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
import logging
import os

from database import get_db, engine, Base, AsyncSessionLocal
from models import Account, Transaction

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transaction_processor")


# ---------------------------------------------------------------------------
# Lifespan – create tables on startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized.")
    except Exception as exc:  # pragma: no cover
        # On a cold-start the DB might not be reachable yet; log and continue
        # so the function doesn't crash before the first real request.
        logger.warning("Could not initialise DB on startup: %s", exc)
    yield
    await engine.dispose()
    logger.info("Database connections closed.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Idempotent Transaction Processor", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class ChargeRequest(BaseModel):
    idempotency_key: uuid.UUID
    account_id: str
    amount: Decimal = Field(..., gt=0)


class AccountCreateRequest(BaseModel):
    account_id: str
    balance: Decimal = Field(..., ge=0)


# ---------------------------------------------------------------------------
# Core charge logic (shared between the API endpoint and the in-process tests)
# ---------------------------------------------------------------------------
async def _do_charge(
    idempotency_key: uuid.UUID,
    account_id: str,
    amount: Decimal,
    db: AsyncSession,
) -> tuple[int, dict]:
    """
    Attempt a charge against *account_id* inside a single DB transaction.

    Returns (http_status_code, response_body_dict).
    Raises IntegrityError on a duplicate-key race – caller must handle.
    """
    async with db.begin():
        # 1. Fast idempotency check
        existing = await db.execute(
            select(Transaction).where(Transaction.idempotency_key == idempotency_key)
        )
        tx = existing.scalar_one_or_none()
        if tx is not None:
            logger.info("Idempotency hit for key %s", idempotency_key)
            return tx.response_code, tx.response_body

        # 2. Row-level lock on the account
        account_result = await db.execute(
            select(Account).where(Account.id == account_id).with_for_update()
        )
        account = account_result.scalar_one_or_none()

        if not account:
            code = status.HTTP_404_NOT_FOUND
            body = {"error": f"Account {account_id} not found"}
            status_str = "FAILED_NOT_FOUND"
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


# ---------------------------------------------------------------------------
# Account endpoints
# ---------------------------------------------------------------------------
@app.post("/accounts")
async def create_account(payload: AccountCreateRequest, db: AsyncSession = Depends(get_db)):
    """Create or reset an account balance."""
    async with db.begin():
        result = await db.execute(select(Account).where(Account.id == payload.account_id))
        account = result.scalar_one_or_none()
        if account:
            account.balance = payload.balance
            msg = "Account updated"
        else:
            account = Account(id=payload.account_id, balance=payload.balance)
            db.add(account)
            msg = "Account created"
    return {"message": msg, "account_id": account.id, "balance": float(account.balance)}


@app.get("/accounts/{account_id}")
async def get_account(account_id: str, db: AsyncSession = Depends(get_db)):
    account = await db.get(Account, account_id)
    if not account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    return {"account_id": account.id, "balance": float(account.balance)}


# ---------------------------------------------------------------------------
# Charge endpoint
# ---------------------------------------------------------------------------
@app.post("/charge")
async def charge(payload: ChargeRequest, db: AsyncSession = Depends(get_db)):
    """
    Deduct balance idempotently using SELECT FOR UPDATE + UNIQUE constraint.
    """
    try:
        code, body = await _do_charge(
            payload.idempotency_key, payload.account_id, payload.amount, db
        )
        return JSONResponse(status_code=code, content=body)

    except IntegrityError as exc:
        # Concurrent request won the race and inserted the key first.
        logger.warning("IntegrityError (race) for key %s: %s", payload.idempotency_key, exc)
        existing = await db.execute(
            select(Transaction).where(Transaction.idempotency_key == payload.idempotency_key)
        )
        tx = existing.scalar_one_or_none()
        if tx is not None:
            return JSONResponse(status_code=tx.response_code, content=tx.response_body)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Conflict during transaction processing. Please retry.",
        )


# ---------------------------------------------------------------------------
# UI helper API
# ---------------------------------------------------------------------------
@app.get("/api/accounts")
async def list_accounts(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Account).order_by(Account.id))
    return [{"account_id": a.id, "balance": float(a.balance)} for a in result.scalars().all()]


@app.get("/api/transactions")
async def list_transactions(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Transaction).order_by(Transaction.id.desc()).limit(50)
    )
    return [
        {
            "id": tx.id,
            "idempotency_key": str(tx.idempotency_key),
            "account_id": tx.account_id,
            "amount": float(tx.amount),
            "status": tx.status,
            "response_code": tx.response_code,
            "response_body": tx.response_body,
        }
        for tx in result.scalars().all()
    ]


# ---------------------------------------------------------------------------
# Concurrency tests – run in-process (no HTTP self-calls)
# ---------------------------------------------------------------------------
async def _run_charges_concurrently(
    account_id: str,
    amount: Decimal,
    keys: list[uuid.UUID],
) -> tuple[int, int]:
    """
    Fire len(keys) charge tasks concurrently, each with its own DB session.
    Returns (success_count, fail_count).
    """
    async def one_charge(key: uuid.UUID) -> bool:
        async with AsyncSessionLocal() as db:
            try:
                code, _ = await _do_charge(key, account_id, amount, db)
                return code == status.HTTP_200_OK
            except IntegrityError:
                # Duplicate key race – look up the winner's result
                existing = await db.execute(
                    select(Transaction).where(Transaction.idempotency_key == key)
                )
                tx = existing.scalar_one_or_none()
                return tx is not None and tx.response_code == status.HTTP_200_OK
            except Exception:
                return False

    results = await asyncio.gather(*[one_charge(k) for k in keys], return_exceptions=False)
    success = sum(1 for r in results if r)
    return success, len(results) - success


@app.post("/api/run-test-1")
async def run_test_1():
    """100 concurrent requests with the SAME idempotency key – balance deducted once."""
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
        result = await db.execute(select(Account).where(Account.id == account_id))
        acc = result.scalar_one_or_none()
    final_balance = float(acc.balance) if acc else 0.0

    return {
        "account_id": account_id,
        "initial_balance": 100.0,
        "charge_amount": 10.0,
        "final_balance": final_balance,
        "success_count": success_count,
        "fail_count": fail_count,
        "expected_final_balance": 90.0,
        "passed": (final_balance == 90.0 and success_count == 100),
    }


@app.post("/api/run-test-2")
async def run_test_2():
    """50 concurrent requests with DIFFERENT keys – all charges applied."""
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
        result = await db.execute(select(Account).where(Account.id == account_id))
        acc = result.scalar_one_or_none()
    final_balance = float(acc.balance) if acc else 0.0
    expected_balance = float(initial_balance - charge_amount * 50)

    return {
        "account_id": account_id,
        "initial_balance": 1000.0,
        "charge_amount": 15.50,
        "final_balance": final_balance,
        "success_count": success_count,
        "fail_count": fail_count,
        "expected_final_balance": expected_balance,
        "passed": (abs(final_balance - expected_balance) < 0.01 and success_count == 50),
    }


# ---------------------------------------------------------------------------
# Static dashboard
# ---------------------------------------------------------------------------
# Vercel serves the /public directory as static assets at the root,
# so "/" → public/index.html is handled by Vercel directly in production.
# When running locally with uvicorn we mount public/ ourselves.
_PUBLIC_DIR = os.path.join(os.path.dirname(__file__), "public")
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")  # legacy path

@app.get("/", response_class=HTMLResponse)
async def get_index():
    """Serve the dashboard – checks public/ then static/ for index.html."""
    for candidate in [
        os.path.join(_PUBLIC_DIR, "index.html"),
        os.path.join(_STATIC_DIR, "index.html"),
    ]:
        if os.path.isfile(candidate):
            with open(candidate, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse(
        content="<h1>Dashboard not found</h1><p>Ensure public/index.html exists.</p>",
        status_code=404,
    )
