from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TransactionEvent(BaseModel):
    user_id: int = Field(..., alias="User")
    card_id: int = Field(..., alias="Card")
    event_timestamp: datetime
    amount: Decimal
    use_chip: str = Field(..., alias="Use Chip")
    merchant_name: str = Field(..., alias="Merchant Name")
    merchant_city: str = Field(..., alias="Merchant City")
    merchant_state: str = Field(..., alias="Merchant State")
    zip_code: Optional[str] = Field(None, alias="Zip")
    mcc: int = Field(..., alias="MCC")
    errors: Optional[str] = Field(None, alias="Errors?")
    is_fraud: bool = Field(..., alias="Is Fraud?")

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, str):
            return v.strip().lower() == "yes"
        return v

    class config:
        population_by_name = True
