from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp

spark = SparkSession.builder \
    .appName("IngestionLakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = """
sensor_id STRING,
type STRING,
value DOUBLE,
unit STRING,
site STRING,
machine STRING,
timestamp STRING
"""

df = spark.readStream \
    .schema(schema) \
    .json("data_lake/raw/*")

df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("year", year("timestamp")) \
       .withColumn("month", month("timestamp")) \
       .withColumn("day", dayofmonth("timestamp"))

query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("site", "type", "year", "month", "day") \
    .option("checkpointLocation", "data_lake/warehouse/checkpoints") \
    .start("data_lake/warehouse/sensors")

query.awaitTermination()
