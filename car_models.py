from config import OUTPUT_PATH, get_spark
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = get_spark(app_name="car_models")

# Build the car models dimension table
car_models_rows = [
    (1, 'Mazda', '3'),
    (2, 'Mazda', '6'),
    (3, 'Toyota', 'Corolla'),
    (4, 'Hyundai', 'i20'),
    (5, 'Kia', 'Sportage'),
    (6, 'Kia', 'Rio'),
    (7, 'Kia', 'Picanto'),
]

car_models_schema = T.StructType([
    T.StructField('model_id', T.IntegerType(), nullable=False),
    T.StructField('car_brand', T.StringType(), nullable=False),
    T.StructField('car_model', T.StringType(), nullable=False),
])

car_models_df = spark.createDataFrame(car_models_rows, schema=car_models_schema)

print("[car_model] car_models table preview:", flush=True)
car_models_df.orderBy(F.col("model_id").asc()).show(truncate=False)


# Write to the requested S3 location
(
    car_models_df.coalesce(1)
    .write.mode("overwrite")
    .parquet("s3a://spark/data/dims/car_models")
)

print("[car_model] Write completed successfully", flush=True)

spark.stop()


