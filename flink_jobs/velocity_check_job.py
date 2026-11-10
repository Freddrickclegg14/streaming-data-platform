import json
import logging
from datetime import datetime

import psycopg2
from pyflink.common import Duration
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from rules import check_velocity

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW_SECONDS = 600
PG_DSN = "dbname=streaming_fraud user=postgres password=postgres host=postgres"
KAFKA_JAR = "file:///opt/flink/lib/custom/flink-sql-connector-kafka-5.0.0-2.1.jar"


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
        existing = list(self.timestamps_state.get()) or []
        is_violation, recent = check_velocity(
            existing, ctx.timestamp(), VELOCITY_WINDOW_SECONDS, VELOCITY_THRESHOLD
        )
        self.timestamps_state.update(recent)
        if is_violation:
            self._write_flag(event["transaction_id"], "velocity_check")

    def _write_flag(self, transaction_id: str, rule: str):
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fraud_flags (transaction_id, flagged_fraud, rule_triggered)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (transaction_id, rule_triggered) DO NOTHING
                """,
                (transaction_id, rule),
            )
        self.pg_conn.commit()


def parse_event(raw: str) -> dict:
    return json.loads(raw)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(KAFKA_JAR)
    env.add_python_file("/opt/flink/usrlib/rules.py")

    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_storage(
        FileSystemCheckpointStorage("file:///opt/flink/usrlib/checkpoints")
    )
    checkpoint_config.set_min_pause_between_checkpoints(30_000)
    checkpoint_config.set_checkpoint_timeout(120_000)
    checkpoint_config.set_max_concurrent_checkpoints(1)

    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))

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
