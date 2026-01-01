# Data ingestion for lakehouse
from spark_session import create_spark_session


def ingest_data(spark, source_path, destination_path):
    """Ingest data from source to destination."""
    df = spark.read.parquet(source_path)
    df.write.mode("overwrite").parquet(destination_path)
    return df


if __name__ == "__main__":
    spark = create_spark_session("IngestionJob")
    print("Ingestion lakehouse module loaded")
