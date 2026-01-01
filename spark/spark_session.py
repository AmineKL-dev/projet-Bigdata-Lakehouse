from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigDataLakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("✅ Spark + Delta Lake prêt")
