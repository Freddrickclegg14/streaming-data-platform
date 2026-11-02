# pyflink/velocity_check_job.py
import json
import logging
from datetime import datetime

import psycopg2
from pyflink.common import Duration
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW_SECONDS = 600
PG_DSN = "dbname=streaming_fraud user=postgres password=postgres host=postgres"
KAFKA_JAR = "file:///C:/Users/User/Desktop/data-engineering-portfolio/streaming-data-platform/pyflink_jars/flink-sql-connector-kafka-5.0.0-2.1.jar"


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event: dict, record_timestamp):
        dt = datetime.fromisoformat(event["event_timestamp"])
        return int(dt.timestamp() * 1000)


class VelocityCheckFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.timestamps_state = runtime_context.get_list_state(
            ListStateDescriptor("recent_timestamps", Types.LONG())
        )
        self.pg_conn = psycopg2.connect(PG_DSN)

    def process_element(self, event: dict, ctx: "KeyedProcessFunction.Context"):
        current_ts = ctx.timestamp()
        window_start = current_ts - (VELOCITY_WINDOW_SECONDS * 1000)

        existing = list(self.timestamps_state.get()) or []
        recent = [ts for ts in existing if ts >= window_start] + [current_ts]
        self.timestamps_state.update(recent)

        if len(recent) >= VELOCITY_THRESHOLD:
            self._write_flag(event["transaction_id"], "velocity_check")

    def _write_flag(self, transaction_id: str, rule: str):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fraud_flags (transaction_id, flagged_fraud, rule_triggered)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (transaction_id, rule),
            )
        self.pg_conn.commit()


def parse_event(raw: str) -> dict:
    return json.loads(raw)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(KAFKA_JAR)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("transactions.raw")
        .set_group_id("velocity-check-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(30)
    ).with_timestamp_assigner(EventTimestampAssigner())

    (
        env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda event: event["user_id"])
        .process(VelocityCheckFunction())
    )

    env.execute("velocity_check_job")


if __name__ == "__main__":
    run()
