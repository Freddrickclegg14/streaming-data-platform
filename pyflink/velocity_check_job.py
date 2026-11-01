import os
import json
import logging
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
import psycopg2

logger = logging.getLogger(__name__)

VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW_MS = 600 * 1000
PG_DSN = os.environ["PG_DSN"]


class VelocityCheckFunction(KeyedProcessFunction):
    def open(self, rc: RuntimeContext):
        self.ts_state = rc.get_list_state(
            ListStateDescriptor("recent_ts", Types.LONG())
        )
        self.flagged_state = rc.get_value_state(
            ValueStateDescriptor("flagged", Types.BOOLEAN())
        )
        self.pg_conn = psycopg2.connect(PG_DSN)

    def close(self):
        if getattr(self, "pg_conn", None):
            self.pg_conn.close()

    def process_element(self, event, ctx):
        now = ctx.timestamp() or 0
        cutoff = now - VELOCITY_WINDOW_MS

        # add current; prune anything older than cutoff
        ts_list = [t for t in (self.ts_state.get() or []) if t >= cutoff]
        ts_list.append(now)
        self.ts_state.update(ts_list)

        already = self.flagged_state.value() or False
        if len(ts_list) >= VELOCITY_THRESHOLD and not already:
            self._write_flags(ts_list, "velocity_check")
            self.flagged_state.update(True)
        elif len(ts_list) < VELOCITY_THRESHOLD:
            self.flagged_state.update(False)

    def _write_flags(self, timestamps, rule):
        # If you need to flag each contributing txn, you must have stored
        # (ts, txn_id) pairs, not just timestamps. Adjust state accordingly.
        ...


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)
    env.add_jars(os.environ.get("FLINK_KAFKA_JAR", "file:///path/to/kafka.jar"))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ.get("KAFKA_BROKERS", "localhost:9092"))
        .set_topics("transactions.raw")
        .set_group_id("velocity-check-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ws = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(30)
    ).with_timestamp_assigner(EventTimestampAssigner())

    stream = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
        .map(lambda raw: json.loads(raw))
        .assign_timestamps_and_watermarks(ws)
        .key_by(lambda e: e["user_id"])
        .process(VelocityCheckFunction())
    )
    env.execute("velocity_check_job")
