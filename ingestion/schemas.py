import hashlib
import math
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _is_nan(v) -> bool:
    return isinstance(v, float) and math.isnan(v)


class TransactionEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    transaction_id: str
    user_id: int = Field(..., alias="User")
    card_id: int = Field(..., alias="Card")
    event_timestamp: datetime
    amount: Decimal = Field(..., alias="Amount")
    use_chip: str = Field(..., alias="Use Chip")
    merchant_id: int = Field(
        ..., alias="Merchant Name"
    )  # numeric ID in this dataset, not a real name
    merchant_city: str = Field(..., alias="Merchant City")
    merchant_state: str | None = Field(
        None, alias="Merchant State"
    )  # None for online txns
    zip_code: str | None = Field(None, alias="Zip")
    mcc: int = Field(..., alias="MCC")
    errors: str | None = Field(None, alias="Errors?")
    # is_fraud intentionally excluded - see GroundTruthLabel

    @model_validator(mode="before")
    @classmethod
    def build_event_timestamp(cls, data):
        if isinstance(data, dict) and "event_timestamp" not in data:
            hour, minute = map(int, str(data["Time"]).split(":"))
            data["event_timestamp"] = datetime(
                int(data["Year"]), int(data["Month"]), int(data["Day"]), hour, minute
            )
        return data

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, str):
            return Decimal(v.replace("$", "").strip())
        return v

    @field_validator("zip_code", mode="before")
    @classmethod
    def parse_zip(cls, v):
        if v is None or _is_nan(v):
            return None
        return str(int(v)) if isinstance(v, float) else str(v)

    @field_validator("merchant_state", "errors", mode="before")
    @classmethod
    def parse_optional_str(cls, v):
        if v is None or _is_nan(v):
            return None
        return str(v)


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
