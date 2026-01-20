from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import (
    KAFKA_ALERT_TOPIC,
    KAFKA_ENRICHED_TOPIC,
    SPARK_KAFKA_PACKAGE,
    get_spark,
    pick_kafka_bootstrap,
)


class AlertDetection:
    def __init__(self) -> None:
        self.bootstrap_servers = pick_kafka_bootstrap()
        self.spark = get_spark(app_name="AlertDetection", spark_packages=SPARK_KAFKA_PACKAGE)
        self.spark.sparkContext.setLogLevel("WARN")

        self._schema = T.StructType(
            [
                T.StructField("event_id", T.StringType(), True),
                T.StructField("event_time", T.StringType(), True),
                T.StructField("car_id", T.IntegerType(), True),
                T.StructField("speed", T.IntegerType(), True),
                T.StructField("rpm", T.IntegerType(), True),
                T.StructField("gear", T.IntegerType(), True),
                T.StructField("driver_id", T.IntegerType(), True),
                T.StructField("brand_name", T.StringType(), True),
                T.StructField("model_name", T.StringType(), True),
                T.StructField("color_name", T.StringType(), True),
                T.StructField("expected_gear", T.IntegerType(), True),
            ]
        )

    def run(self) -> None:
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", KAFKA_ENRICHED_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        parsed_df = (
            kafka_df.select(F.col("value").cast("string").alias("value"))
            .withColumn("json", F.from_json(F.col("value"), self._schema))
            .select("json.*")
        )

        alerts_df = parsed_df.where(
            (F.col("speed") > F.lit(120))
            | (F.col("expected_gear") != F.col("gear"))
            | (F.col("rpm") > F.lit(6000))
        )

        out_df = alerts_df.select(
            F.to_json(F.struct(*[F.col(c) for c in alerts_df.columns])).alias("value")
        )

        checkpoint = "checkpoints/mid_project_car/alert_detection"

        query_kafka = (
            out_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", KAFKA_ALERT_TOPIC)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start()
        )

        # Optional console preview (handy during development)
        (
            alerts_df.writeStream.format("console")
            .option("truncate", "false")
            .outputMode("append")
            .start()
        )

        query_kafka.awaitTermination()


if __name__ == "__main__":
    AlertDetection().run()
