import json
import logging
from datetime import datetime
from decimal import Decimal

import pandas as pd
import psycopg2
from confluent_kafka import Producer
from psycopg2.extras import execute_values
from pydantic import ValidationError

from ingestion.schemas import GroundTruthLabel, TransactionEvent, build_transaction_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_TOPIC = "transaction.raw"
DEADLETTER_TOPIC = "transaction.deadletter"
BATCH_SIZE = 10_000


def default_json(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)


def build_deadletter_record(raw_row: dict, error: Exception) -> dict:
    return {
        "failed at": datetime.utcnow().isoformat(),
        "error_reason": str(error),
        "raw_record": raw_row,
    }


def flush_labels(pg_conn, labels: list[GroundTruthLabel]):
    if not labels:
        return
    with pg_conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ground_truth_labels (transaction_id, is_fraud)
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [(lbl.transaction_id, lbl.is_fraud) for lbl in labels],
        )
    pg_conn.commit()


def produce_from_csv(
    csv_path: str,
    bootstrap_servers: str = "localhost:9092",
    pg_dsn: str = "dbname=streaming_fraud user=postgres host=localhost",
):
    producer = Producer({"bootstrap_servers": bootstrap_servers})
    pg_conn = psycopg2.connect(pg_dsn)
    df = pd.read_csv(csv_path)

    sent, deadlettered = 0, 0
    label_buffer: list[GroundTruthLabel] = []

    for idx, row in enumerate(df.to_dict(orient="records")):
        txn_id = build_transaction_id(idx, row)

        try:
            label_buffer.append(
                GroundTruthLabel(Transaction_id=txn_id, is_fraud=row["Is Fraud?"])
            )
        except ValidationError as e:
            logger.warning("Could not parse label for row %d: %s", idx, e)

        if len(label_buffer) >= BATCH_SIZE:
            flush_labels(pg_conn, label_buffer)
            label_buffer.clear()

        try:
            event = TransactionEvent(transaction_id=txn_id, **row)
            producer.produce(
                topic=RAW_TOPIC,
                key=str(event.user_id),
                value=json.dumps(
                    event.model_dump(by_alias=False), default=default_json
                ),
                callback=delivery_report,
            )
            sent += 1
        except (ValidationError, ValueError, TypeError) as e:
            dl_record = build_deadletter_record(row, e)
            producer.produce(
                topic=DEADLETTER_TOPIC,
                value=json.dumps(dl_record, default=str),
                callback=delivery_report,
            )
            deadlettered += 1
            logger.warning("Row sent to deadletter: %s", e)

        producer.poll(0)

    flush_labels(pg_conn, label_buffer)
    producer.flush()
    pg_conn.close()
    logger.info("Done. sent=%d deadlettered=%d", sent, deadlettered)
