# Spark session configuration
from pyspark.sql import SparkSession


def create_spark_session(app_name="DataLakeApp"):
    """Create and return a Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    print("Spark session created successfully")
