from sqlalchemy import Column, String, Numeric, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from database import Base

class Account(Base):
    __tablename__ = "accounts"
    
    id = Column(String, primary_key=True, index=True)
    balance = Column(Numeric(12, 2), nullable=False)

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    idempotency_key = Column(UUID(as_uuid=True), unique=True, nullable=False, index=True)
    account_id = Column(String, ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    status = Column(String, nullable=False)
    response_code = Column(Integer, nullable=False)
    response_body = Column(JSONB, nullable=False)
