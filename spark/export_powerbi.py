from pyspark.sql import SparkSession
import os

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("ExportPowerBI") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark prêt pour export Power BI")

# --------------------------------------------------
# 2. Dossiers
# --------------------------------------------------
WAREHOUSE_PATH = "data_lake/warehouse"
EXPORT_PATH = "data_lake/powerbi"

tables = {
    "avg_temperature": "avg_temperature",
    "alerts": "alerts",
    "top_vibration": "top_vibration",
    "pressure_hourly": "pressure_hourly"
}

# Création du dossier powerbi si nécessaire
os.makedirs(EXPORT_PATH, exist_ok=True)

# --------------------------------------------------
# 3. Fonction d’export Delta → CSV
# --------------------------------------------------
def export_table(table_name):
    print(f"Export de la table : {table_name}")

    df = spark.read.format("delta") \
        .load(f"{WAREHOUSE_PATH}/{table_name}")

    output_path = f"{EXPORT_PATH}/{table_name}"

    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    print(f"Table {table_name} exportée vers {output_path}")

# --------------------------------------------------
# 4. Export de toutes les tables
# --------------------------------------------------
for table in tables.values():
    export_table(table)

# --------------------------------------------------
# 5. Fin
# --------------------------------------------------
print("Export Power BI terminé avec succès")

spark.stop()
