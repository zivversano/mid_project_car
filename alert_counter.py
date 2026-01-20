import os
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import SPARK_KAFKA_PACKAGE, get_spark, pick_kafka_bootstrap


def _split_topics(value: str) -> list[str]:
    return [t.strip() for t in value.split(",") if t.strip()]


def _kafka_list_topics(bootstrap_servers: str) -> set[str]:
    # Use kafka-python (already a dependency in this repo) to query metadata.
    from kafka import KafkaConsumer

    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    if not servers:
        return set()

    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=4000,
        metadata_max_age_ms=5000,
        connections_max_idle_ms=9000,
    )
    try:
        return set(consumer.topics() or [])
    finally:
        try:
            consumer.close(timeout=5)
        except Exception:
            pass


def _kafka_create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic

    servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    if not servers or not topics:
        return

    admin = KafkaAdminClient(
        bootstrap_servers=servers,
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=5000,
    )
    try:
        new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]
        # Kafka will throw if topics already exist; ignore those errors.
        admin.create_topics(new_topics=new_topics, validate_only=False)
    finally:
        try:
            admin.close()
        except Exception:
            pass


class AlertCounter:
    def __init__(self) -> None:
        self.bootstrap_servers = pick_kafka_bootstrap()
        self.spark = get_spark(app_name="AlertCounter", spark_packages=SPARK_KAFKA_PACKAGE)
        self.spark.sparkContext.setLogLevel("WARN")

        
        self._schema = T.StructType(
            [
                T.StructField("event_time", T.StringType(), True),
                T.StructField("speed", T.IntegerType(), True),
                T.StructField("gear", T.IntegerType(), True),
                T.StructField("rpm", T.IntegerType(), True),
                T.StructField("color", T.StringType(), True),
                T.StructField("color_name", T.StringType(), True),
            ]
        )

    def run(self) -> None:
        # Default input topics per assignment; allow override via env.
        topics_raw = os.getenv("ALERT_COUNTER_TOPICS", "anomaly-alerts,alert-data")
        requested = _split_topics(topics_raw)
        if not requested:
            raise RuntimeError("ALERT_COUNTER_TOPICS is empty")

        create_missing = os.getenv("KAFKA_CREATE_MISSING_TOPICS", "0").strip() == "1"
        wait_for_topics = os.getenv("KAFKA_WAIT_FOR_TOPICS", "1").strip() == "1"
        poll_seconds = int(os.getenv("KAFKA_WAIT_FOR_TOPICS_POLL_SECONDS", "5") or "5")
        max_wait_seconds = int(os.getenv("KAFKA_WAIT_FOR_TOPICS_SECONDS", "0") or "0")

        started_wait = time.monotonic()

        while True:
            existing = _kafka_list_topics(self.bootstrap_servers)
            missing = [t for t in requested if t not in existing]

            if missing and create_missing:
                print(f"[AlertCounter] Creating missing topics: {missing}", flush=True)
                _kafka_create_topics(self.bootstrap_servers, missing)
                existing = _kafka_list_topics(self.bootstrap_servers)
                missing = [t for t in requested if t not in existing]

            effective = [t for t in requested if t in existing]
            if effective:
                if missing:
                    print(
                        "[AlertCounter] WARN: Some input topics do not exist in Kafka and will be ignored: "
                        f"{missing}. Existing topics: {sorted(existing)}",
                        flush=True,
                    )
                break

            if not wait_for_topics:
                raise RuntimeError(
                    "None of the requested topics exist in Kafka. "
                    f"requested={requested} existing={sorted(existing)}. "
                    "Start upstream jobs (AlertDetection/AnomalyDetection) or set KAFKA_CREATE_MISSING_TOPICS=1."
                )

            elapsed = time.monotonic() - started_wait
            if max_wait_seconds > 0 and elapsed >= max_wait_seconds:
                raise RuntimeError(
                    "Timed out waiting for requested topics to appear in Kafka. "
                    f"requested={requested} existing={sorted(existing)}"
                )

            print(
                "[AlertCounter] Waiting for topics to exist in Kafka... "
                f"requested={requested} existing={sorted(existing)}",
                flush=True,
            )
            time.sleep(max(1, poll_seconds))

        topics = ",".join(effective)

        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topics)
            .option("startingOffsets", "latest")
            .load()
        )

        parsed = (
            kafka_df.select(
                F.col("topic"),
                F.col("timestamp").alias("kafka_ts"),
                F.col("value").cast("string").alias("value"),
            )
            .withColumn("json", F.from_json(F.col("value"), self._schema))
            .select("topic", "kafka_ts", F.col("json.*"))
        )

        # Use event_time when present, otherwise fall back to Kafka message timestamp.
        with_ts = parsed.withColumn(
            "event_ts",
            F.coalesce(F.to_timestamp(F.col("event_time")), F.col("kafka_ts")),
        )

        # Normalize color (some pipelines use color_name, others may use color).
        normalized = (
            with_ts.withColumn(
                "color_norm",
                F.lower(F.coalesce(F.col("color_name"), F.col("color"))),
            )
            .withColumn("speed_i", F.col("speed").cast("int"))
            .withColumn("gear_i", F.col("gear").cast("int"))
            .withColumn("rpm_i", F.col("rpm").cast("int"))
            .where(F.col("event_ts").isNotNull())
        )

        # Rolling last 15 minutes, updated every minute.
        windowed = (
            normalized.withWatermark("event_ts", "20 minutes")
            .groupBy(F.window(F.col("event_ts"), "15 minutes", "1 minute"))
            .agg(
                F.count(F.lit(1)).alias("num_of_rows"),
                F.sum(F.when(F.col("color_norm") == F.lit("black"), 1).otherwise(0)).cast("long").alias(
                    "num_of_black"
                ),
                F.sum(F.when(F.col("color_norm") == F.lit("white"), 1).otherwise(0)).cast("long").alias(
                    "num_of_white"
                ),
                F.sum(F.when(F.col("color_norm") == F.lit("silver"), 1).otherwise(0)).cast("long").alias(
                    "num_of_silver"
                ),
                F.max(F.col("speed_i")).alias("maximum_speed"),
                F.max(F.col("gear_i")).alias("maximum_gear"),
                F.max(F.col("rpm_i")).alias("maximum_rpm"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "num_of_rows",
                "num_of_black",
                "num_of_white",
                "num_of_silver",
                "maximum_speed",
                "maximum_gear",
                "maximum_rpm",
            )
        )

        # Print a single (latest) 15-minute window aggregation.
        latest = windowed.orderBy(F.col("window_end").desc()).limit(1)

        checkpoint = "checkpoints/mid_project_car/alert_counter"

        (
            latest.writeStream.format("console")
            .option("truncate", "false")
            .option("numRows", "50")
            .option("checkpointLocation", checkpoint)
            .outputMode("complete")
            .trigger(processingTime="60 seconds")
            .start()
            .awaitTermination()
        )


if __name__ == "__main__":
    AlertCounter().run()
