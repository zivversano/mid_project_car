import os
import random

from config import get_spark
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _generate_unique_numbers(count: int, digits: int, rng: random.Random) -> list[int]:
    if digits <= 0:
        raise ValueError("digits must be positive")

    low = 10 ** (digits - 1)
    high = (10**digits) - 1

    # Avoid random.sample on a huge range for portability; generate until unique.
    seen: set[int] = set()
    out: list[int] = []
    while len(out) < count:
        n = rng.randint(low, high)
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def main() -> None:
    spark = get_spark(app_name="generate_cars")
    count = _env_int("CARS_COUNT", 20)
    seed = os.getenv("CARS_SEED")
    rng = random.Random(int(seed)) if seed is not None else random.Random()

    car_ids = _generate_unique_numbers(count=count, digits=7, rng=rng)
    driver_ids = _generate_unique_numbers(count=count, digits=9, rng=rng)

    rows = [
        (
            int(car_ids[i]),
            int(driver_ids[i]),
            int(rng.randint(1, 7)),
            int(rng.randint(1, 7)),
        )
        for i in range(count)
    ]

    schema = T.StructType(
        [
            T.StructField("car_id", T.IntegerType(), nullable=False),
            T.StructField("driver_id", T.IntegerType(), nullable=False),
            T.StructField("model_id", T.IntegerType(), nullable=False),
            T.StructField("color_id", T.IntegerType(), nullable=False),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)

    # Safety: assert uniqueness of car_id inside Spark too.
    duplicates = df.groupBy(F.col("car_id")).count().where(F.col("count") > 1)
    if duplicates.take(1):
        raise RuntimeError("car_id is not unique; generation logic failed")

    print("[generate_cars] Preview:")
    df.orderBy(F.col("car_id").asc()).show(n=count, truncate=False)

    out_path = os.getenv("CARS_OUT_PATH", "s3a://spark/data/dims/cars")
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .parquet(out_path)
    )
    print(f"[generate_cars] Wrote parquet to {out_path}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
