import json
import time
import uuid
from datetime import datetime, timezone
from time import sleep

from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

from config import (
    CARS_INPUT_PATH,
    KAFDROP_URL,
    KAFKA_RECONNECT_DELAY_SECONDS,
    KAFKA_TOPIC,
    PREVIEW_COUNT,
    REQUIRE_KAFKA,
    create_kafka_producer,
    get_spark,
    pick_kafka_bootstrap,
)


class DataGenerator:
    def __init__(self) -> None:
        self.input_path = CARS_INPUT_PATH
        self.kafka_topic = KAFKA_TOPIC
        self.bootstrap_servers = pick_kafka_bootstrap()

        self.spark = get_spark(app_name="DataGenerator")

        cars_df = self.spark.read.parquet(self.input_path)
        if "car_id" not in cars_df.columns:
            raise RuntimeError(
                f"Expected a 'car_id' column in {self.input_path}. Found: {cars_df.columns}"
            )

        self.car_ids: list[int] = [
            int(r["car_id"]) for r in cars_df.select("car_id").distinct().collect()
        ]
        if not self.car_ids:
            raise RuntimeError(f"No car_id rows found in {self.input_path}")

        self.producer = create_kafka_producer(self.bootstrap_servers)
        self._next_reconnect_at = 0.0

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _rand_int(low: int, high: int) -> int:
        import random

        return random.randint(low, high)

    def _build_event(self, car_id: int) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "event_time": self._now_iso(),
            "car_id": int(car_id),
            "speed": self._rand_int(0, 200),
            "rpm": self._rand_int(0, 8000),
            "gear": self._rand_int(1, 7),
        }

    def _ensure_producer(self) -> None:
        if self.producer is not None:
            return

        now = __import__("time").time()
        if now < self._next_reconnect_at:
            return

        self.producer = create_kafka_producer(self.bootstrap_servers)
        self._next_reconnect_at = now + KAFKA_RECONNECT_DELAY_SECONDS

    def run(self) -> None:
        print(
            f"[DataGenerator] Loaded {len(self.car_ids)} cars from {self.input_path}; "
            f"topic={self.kafka_topic} bootstrap={self.bootstrap_servers}",
            flush=True,
        )
        print(f"[DataGenerator] Kafdrop UI: {KAFDROP_URL}", flush=True)
        print(
            f"[DataGenerator] Console preview enabled: first {PREVIEW_COUNT} events per second",
            flush=True,
        )

        try:
            while True:
                self._ensure_producer()
                sent_count = 0

                for idx, car_id in enumerate(self.car_ids):
                    payload = json.dumps(self._build_event(car_id), separators=(",", ":"))

                    if PREVIEW_COUNT > 0 and idx < PREVIEW_COUNT:
                        print(f"[Preview] {payload}", flush=True)

                    if self.producer is None:
                        continue

                    try:
                        self.producer.send(self.kafka_topic, value=payload)
                        sent_count += 1
                    except (KafkaTimeoutError, NoBrokersAvailable, KafkaError) as exc:
                        print(f"[DataGenerator] Kafka send failed: {exc}", flush=True)
                        try:
                            self.producer.close(timeout=5)
                        except Exception:
                            pass
                        self.producer = None
                        break

                if self.producer is not None:
                    try:
                        self.producer.flush()
                    except (KafkaTimeoutError, NoBrokersAvailable, KafkaError) as exc:
                        print(f"[DataGenerator] Kafka flush failed: {exc}", flush=True)
                        try:
                            self.producer.close(timeout=5)
                        except Exception:
                            pass
                        self.producer = None

                if self.producer is None:
                    if REQUIRE_KAFKA:
                        raise RuntimeError("Kafka is required but not available")
                    print(
                        "[DataGenerator] Kafka not available; printed preview only this second",
                        flush=True,
                    )
                else:
                    print(f"[DataGenerator] Sent {sent_count} events", flush=True)

                sleep(1)
        except KeyboardInterrupt:
            print("[DataGenerator] Stopped (KeyboardInterrupt)", flush=True)
        finally:
            try:
                if self.producer is not None:
                    self.producer.flush(timeout=10)
            except Exception:
                pass
            try:
                if self.producer is not None:
                    self.producer.close(timeout=10)
            except Exception:
                pass
            try:
                self.spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    DataGenerator().run()
