import requests
import json
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# --------------------------------------------------
# CONFIG POWER BI
# --------------------------------------------------
POWERBI_URL = "https://api.powerbi.com/beta/dc59e38c-4977-406f-bdd1-9ebbabbd387e/datasets/16a39e7f-1971-4fd8-8c57-7bd811856794/rows?experience=power-bi&key=Iort9%2BWdnwHZ03BIwLvhCz3mkhGdCkBk7Zb0r%2BD%2B3%2B4c6ktPP8wF3%2FISb%2FJmOnulLVx%2FzOfxzEirK1zI2iILfQ%3D%3D"

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("PushAllLive") \
    .getOrCreate()

print("✅ Spark démarré – Push LIVE vers Power BI")

WAREHOUSE = "data_lake/warehouse"

# --------------------------------------------------
# Fonction utilitaire : vérifier existence table Delta
# --------------------------------------------------
def table_exists(path):
    return os.path.exists(path)

# --------------------------------------------------
# Fonction PUSH Power BI
# --------------------------------------------------
def push(rows):
    if not rows:
        return
    r = requests.post(
        POWERBI_URL,
        json=rows,
        headers={"Content-Type": "application/json"}
    )
    print("📤 PUSH status:", r.status_code)

# --------------------------------------------------
# LOOP LIVE
# --------------------------------------------------
while True:

    payload = []

    # -------------------------------
    # 1. Température moyenne
    # -------------------------------
    path = f"{WAREHOUSE}/avg_temperature"
    if table_exists(path):
        df = spark.read.format("delta").load(path) \
            .withColumn("timestamp", current_timestamp())

        for r in df.collect():
            payload.append({
                "metric": "avg_temperature",
                "site": r["site"],
                "machine": r["machine"],
                "value": r["avg_temperature"],
                "timestamp": r["timestamp"]
            })

    # -------------------------------
    # 2. Alertes
    # -------------------------------
    path = f"{WAREHOUSE}/alerts"
    if table_exists(path):
        df = spark.read.format("delta").load(path) \
            .withColumn("timestamp", current_timestamp())

        for r in df.collect():
            payload.append({
                "metric": "alerts",
                "site": "ALL",
                "machine": r["type"],
                "value": r["nb_alertes"],
                "timestamp": r["timestamp"]
            })

    # -------------------------------
    # 3. Top vibration
    # -------------------------------
    path = f"{WAREHOUSE}/top_vibration"
    if table_exists(path):
        df = spark.read.format("delta").load(path) \
            .withColumn("timestamp", current_timestamp())

        for r in df.collect():
            payload.append({
                "metric": "vibration_variability",
                "site": "ALL",
                "machine": r["machine"],
                "value": r["vibration_variability"],
                "timestamp": r["timestamp"]
            })

    # -------------------------------
    # 4. Pression horaire
    # -------------------------------
    path = f"{WAREHOUSE}/pressure_hourly"
    if table_exists(path):
        df = spark.read.format("delta").load(path) \
            .withColumn("timestamp", current_timestamp())

        for r in df.collect():
            payload.append({
                "metric": "avg_pressure",
                "site": r["site"],
                "machine": f"hour_{r['hour']}",
                "value": r["avg_pressure"],
                "timestamp": r["timestamp"]
            })

    # -------------------------------
    # PUSH GLOBAL
    # -------------------------------
    push(payload)

    time.sleep(10)
