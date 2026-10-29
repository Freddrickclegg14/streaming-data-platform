import hashlib
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TransactionEvent(BaseModel):
    transaction_id: str
    user_id: int = Field(..., alias="User")
    card_id: int = Field(..., alias="Card")
    event_timestamp: datetime
    amount: Decimal = Field(..., alias="Amount")
    use_chip: str = Field(..., alias="Use Chip")
    merchant_name: str = Field(..., alias="Merchant Name")
    merchant_city: str = Field(..., alias="Merchant City")
    merchant_state: str = Field(..., alias="Merchant State")
    zip_code: str | None = Field(None, alias="Zip")
    mcc: int = Field(..., alias="MCC")
    errors: str | None = Field(None, alias="Errors?")
    # is_fraud: bool = Field(..., alias="Is Fraud?")

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, str):
            return Decimal(v.replace("$", "").strip())
        return v

    class Config:
        population_by_name = True


class GroundTruthLabel(BaseModel):
    transaction_id: str
    is_fraud: bool

    @field_validator("is_fraud", mode="before")
    @classmethod
    def parse_fraud_flag(cls, v):
        if isinstance(v, str):
            return v.strip().lower() == "yes"
        return v


def build_transaction_id(row_index, raw_row: dict) -> str:
    basis = f"{row_index}|{raw_row}".encode()
    return hashlib.sha256(basis).hexdigest()[:16]
