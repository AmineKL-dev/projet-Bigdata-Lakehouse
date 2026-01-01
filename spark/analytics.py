from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("LakehouseAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --------------------------------------------------
# 2. Lecture des données Delta (Lakehouse)
# --------------------------------------------------
sensors_df = spark.read.format("delta") \
    .load("data_lake/warehouse/sensors")

sensors_df.createOrReplaceTempView("sensors")

print("✅ Données Lakehouse chargées")

# --------------------------------------------------
# 3. ANALYSE 1 : Température moyenne par site et machine
# --------------------------------------------------
avg_temperature_df = spark.sql("""
SELECT
    site,
    machine,
    AVG(value) AS avg_temperature
FROM sensors
WHERE type = 'temperature'
GROUP BY site, machine
""")

avg_temperature_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data_lake/warehouse/avg_temperature")

print("✅ Table avg_temperature créée")

# --------------------------------------------------
# 4. ANALYSE 2 : Nombre d’alertes critiques par type
# --------------------------------------------------
alerts_df = spark.sql("""
SELECT
    type,
    COUNT(*) AS nb_alertes
FROM sensors
WHERE
    (type = 'temperature' AND value > 80)
    OR (type = 'vibration' AND value > 40)
    OR (type = 'pressure' AND value < 950)
GROUP BY type
""")

alerts_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data_lake/warehouse/alerts")

print("✅ Table alerts créée")

# --------------------------------------------------
# 5. ANALYSE 3 : Top 5 machines (variabilité vibration)
# --------------------------------------------------
top_vibration_df = spark.sql("""
SELECT
    machine,
    STDDEV(value) AS vibration_variability
FROM sensors
WHERE type = 'vibration'
GROUP BY machine
ORDER BY vibration_variability DESC
LIMIT 5
""")

top_vibration_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data_lake/warehouse/top_vibration")

print("✅ Table top_vibration créée")

# --------------------------------------------------
# 6. ANALYSE 4 : Évolution horaire pression moyenne
# --------------------------------------------------
pressure_hourly_df = spark.sql("""
SELECT
    site,
    hour(timestamp) AS hour,
    AVG(value) AS avg_pressure
FROM sensors
WHERE type = 'pressure'
GROUP BY site, hour(timestamp)
ORDER BY site, hour
""")

pressure_hourly_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("data_lake/warehouse/pressure_hourly")

print("✅ Table pressure_hourly créée")

# --------------------------------------------------
# 7. Fin
# --------------------------------------------------
print("🎉 Toutes les analyses ont été générées avec succès")

spark.stop()
