from decimal import Decimal

import pytest
from pydantic import ValidationError

from ingestion.schemas import GroundTruthLabel, TransactionEvent, build_transaction_id

VALID_ROW = {
    "User": 0,
    "Card": 0,
    "Use Chip": "Swipe Transaction",
    "Merchant Name": "1234567890",
    "Merchant City": "La Verne",
    "Merchant State": "CA",
    "Zip": "91750.0",
    "MCC": 5300,
    "Errors?": None,
    "Amount": "$123.45",
}


def test_valid_row_parses_and_excludes_is_fraud():
    event = TransactionEvent(
        transaction_id="abc123",
        event_timestamp="2002-09-01T06:21:00",
        **VALID_ROW,
    )
    assert event.amount == Decimal("123.45")
    assert not hasattr(event, "is_fraud")  # the whole point of the refactor


def test_malformed_amount_raises():
    bad_row = {**VALID_ROW, "Amount": "not-a-number"}
    with pytest.raises(ValidationError):
        TransactionEvent(
            transaction_id="abc123", event_timestamp="2002-09-01T06:21:00", **bad_row
        )


def test_transaction_id_is_deterministic():
    row = {"a": 1, "b": 2}
    assert build_transaction_id(0, row) == build_transaction_id(0, row)
    assert build_transaction_id(0, row) != build_transaction_id(1, row)


def test_label_parses_yes_no_strings():
    assert GroundTruthLabel(transaction_id="x", is_fraud="Yes").is_fraud is True
    assert GroundTruthLabel(transaction_id="x", is_fraud="No").is_fraud is False
