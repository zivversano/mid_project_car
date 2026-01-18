"""DataEnrichment

Object Name: DataEnrichment
Input KAFKA topic: sensors-sample
Output KAFKA topic: samples-enriched

Logic:
- Add to each event:
  - driver_id
  - brand_name
  - model_name
  - color_name
  - expected_gear = round(speed/30)

Run from this folder so local imports work:
  cd mid_project_car && /bin/python3 data_enrichment.py
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import (
    CAR_COLORS_PATH,
    CAR_MODELS_PATH,
    CARS_INPUT_PATH,
    KAFKA_ENRICHED_TOPIC,
    KAFKA_TOPIC,
    SPARK_KAFKA_PACKAGE,
    get_spark,
    pick_kafka_bootstrap,
)


class DataEnrichment:
    def __init__(self) -> None:
        self.bootstrap_servers = pick_kafka_bootstrap()
        self.spark = get_spark(app_name="DataEnrichment", spark_packages=SPARK_KAFKA_PACKAGE)
        self.spark.sparkContext.setLogLevel("WARN")

        self._event_schema = T.StructType(
            [
                T.StructField("event_id", T.StringType(), True),
                T.StructField("event_time", T.StringType(), True),
                T.StructField("car_id", T.IntegerType(), True),
                T.StructField("speed", T.IntegerType(), True),
                T.StructField("rpm", T.IntegerType(), True),
                T.StructField("gear", T.IntegerType(), True),
            ]
        )

    def run(self) -> None:
        cars_df = (
            self.spark.read.parquet(CARS_INPUT_PATH)
            .select("car_id", "driver_id", "model_id", "color_id")
            .dropDuplicates(["car_id"])
        )

        models_df = (
            self.spark.read.parquet(CAR_MODELS_PATH)
            .select(
                F.col("model_id"),
                F.col("car_brand").alias("brand_name"),
                F.col("car_model").alias("model_name"),
            )
            .dropDuplicates(["model_id"])
        )

        colors_df = (
            self.spark.read.parquet(CAR_COLORS_PATH)
            .select("color_id", "color_name")
            .dropDuplicates(["color_id"])
        )

        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        parsed_df = (
            kafka_df.select(F.col("value").cast("string").alias("value"))
            .withColumn("json", F.from_json(F.col("value"), self._event_schema))
            .select("json.*")
        )

        enriched_df = (
            parsed_df.join(cars_df, on="car_id", how="left")
            .join(models_df, on="model_id", how="left")
            .join(colors_df, on="color_id", how="left")
            .withColumn(
                "expected_gear",
                F.round(F.col("speed").cast("double") / F.lit(30.0)).cast("int"),
            )
            .select(
                "event_id",
                "event_time",
                "car_id",
                "speed",
                "rpm",
                "gear",
                "driver_id",
                "brand_name",
                "model_name",
                "color_name",
                "expected_gear",
            )
        )

        out_df = enriched_df.select(F.to_json(F.struct(*[F.col(c) for c in enriched_df.columns])).alias("value"))

        checkpoint = "checkpoints/mid_project_car/data_enrichment"

        query_kafka = (
            out_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", KAFKA_ENRICHED_TOPIC)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start()
        )

        # console preview of enriched rows
        (
            enriched_df.writeStream.format("console")
            .option("truncate", "false")
            .outputMode("append")
            .start()
        )

        query_kafka.awaitTermination()


if __name__ == "__main__":
    DataEnrichment().run()
