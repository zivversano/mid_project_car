import os

from config import get_spark
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = get_spark(app_name="car_colors")

# Output location for the dim table
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://spark/data/dims/car_colors")

# Build the car models dimension table
car_colors_rows = [
    (1, 'Black'),
    (2, 'Red'),
    (3, 'Gray'),
    (4, 'White'),
    (5, 'Green'),
    (6, 'Blue'),
    (7, 'Pink'),
]

car_colors_schema = T.StructType([
    T.StructField('color_id', T.IntegerType(), nullable=False),
    T.StructField('color_name', T.StringType(), nullable=False),
])

car_colors_df = spark.createDataFrame(car_colors_rows, schema=car_colors_schema)

print("[car_colors] car_colors table preview:", flush=True)

car_colors_df.orderBy(F.col("color_id").asc()).show(truncate=False)


# Write to the requested S3 location
(
    car_colors_df.coalesce(1)
    .write.mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("[car_colors] Write completed successfully", flush=True)

spark.stop()