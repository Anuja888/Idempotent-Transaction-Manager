from sqlalchemy import Column, String, Numeric, Integer, ForeignKey, DateTime, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from database import Base


class Account(Base):
    __tablename__ = "accounts"

    id = Column(String, primary_key=True, index=True)
    balance = Column(Numeric(12, 2), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    idempotency_key = Column(UUID(as_uuid=True), unique=True, nullable=False, index=True)
    account_id = Column(
        String, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    amount = Column(Numeric(12, 2), nullable=False)
    status = Column(String, nullable=False)  # SUCCESS | FAILED_INSUFFICIENT_FUNDS | FAILED_NOT_FOUND
    response_code = Column(Integer, nullable=False)
    response_body = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
