#!/usr/bin/env python3
"""
Analyse décisionnelle - InduSense Data Lakehouse
"""

import os
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# PATHS
# ------------------------------------------------------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_PATH = os.path.join(BASE_PATH, "data_lake/warehouse/sensors")
REPORTS_PATH = os.path.join(BASE_PATH, "reports")

os.makedirs(REPORTS_PATH, exist_ok=True)
os.makedirs("C:/stmp", exist_ok=True)

# ------------------------------------------------------------------
# ALERT THRESHOLDS
# ------------------------------------------------------------------
ALERT_THRESHOLDS = {
    "temperature": {"min": 0, "max": 85},
    "vibration": {"min": 0, "max": 7.0},
    "pressure": {"min": 0.5, "max": 6.0}
}


class LakehouseAnalytics:

    def __init__(self):
        logger.info("🚀 Initialisation Spark Analytics...")

        builder = (
            SparkSession.builder
            .appName("InduSense_Analytics")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            # ✅ FIX WINDOWS TEMP
            .config("spark.local.dir", "C:/stmp")

            # ✅ MASQUER WARN SPARK
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configurationFile=file:/C:/stmp/log4j2.properties"
            )            
            .config("spark.executor.extraJavaOptions", "-Dlog4j2.formatMsgNoLookups=true")

            .config("spark.ui.enabled", "false")
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.setLogLevel("FATAL")

        self._load_data()
        logger.info("✅ Environnement prêt")

    # ------------------------------------------------------------------
    def _load_data(self):
        if not os.path.exists(WAREHOUSE_PATH):
            raise FileNotFoundError("❌ Warehouse introuvable. Lance d’abord le pipeline.")

        self.df = self.spark.read.format("delta").load(WAREHOUSE_PATH)
        self.df.createOrReplaceTempView("sensor_data")

        logger.info(f"📦 {self.df.count()} enregistrements chargés")

    # ------------------------------------------------------------------
    def _write_csv(self, df, name):
        path = os.path.join(REPORTS_PATH, name)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(path)
        )
        logger.info(f"💾 Rapport généré: {path}")

    # ==================================================================
    # ANALYSE 1
    # ==================================================================
    def analyse_temperature_moyenne(self):
        logger.info("📊 Analyse température moyenne")

        query = """
        SELECT
            site,
            machine,
            date_format(timestamp, 'yyyy-MM-dd') AS date,
            ROUND(AVG(value), 2) AS temperature_moyenne,
            ROUND(MIN(value), 2) AS temperature_min,
            ROUND(MAX(value), 2) AS temperature_max,
            COUNT(*) AS nombre_mesures
        FROM sensor_data
        WHERE type = 'temperature'
        GROUP BY site, machine, date_format(timestamp, 'yyyy-MM-dd')
        ORDER BY site, machine
        """

        result = self.spark.sql(query)
        result.show(truncate=False)
        self._write_csv(result, "temperature_moyenne")
        return result

    # ==================================================================
    # ANALYSE 2
    # ==================================================================
    def analyse_alertes_critiques(self):
        logger.info("🚨 Analyse alertes critiques")

        query = f"""
        SELECT
            type,
            COUNT(*) AS total_mesures,
            SUM(
                CASE
                    WHEN type='temperature' AND (value<{ALERT_THRESHOLDS['temperature']['min']} OR value>{ALERT_THRESHOLDS['temperature']['max']}) THEN 1
                    WHEN type='vibration' AND (value<{ALERT_THRESHOLDS['vibration']['min']} OR value>{ALERT_THRESHOLDS['vibration']['max']}) THEN 1
                    WHEN type='pressure' AND (value<{ALERT_THRESHOLDS['pressure']['min']} OR value>{ALERT_THRESHOLDS['pressure']['max']}) THEN 1
                    ELSE 0
                END
            ) AS alertes_critiques
        FROM sensor_data
        GROUP BY type
        ORDER BY alertes_critiques DESC
        """

        result = self.spark.sql(query)
        result.show(truncate=False)
        self._write_csv(result, "alertes_critiques")
        return result

    # ==================================================================
    # ANALYSE 3
    # ==================================================================
    def analyse_variabilite_vibration(self):
        logger.info("📳 Analyse variabilité vibration")

        query = """
        SELECT
            site,
            machine,
            COUNT(*) AS nombre_mesures,
            ROUND(STDDEV(value), 3) AS ecart_type,
            ROUND(MIN(value), 3) AS min_val,
            ROUND(MAX(value), 3) AS max_val
        FROM sensor_data
        WHERE type='vibration'
        GROUP BY site, machine
        HAVING COUNT(*) >= 10
        ORDER BY ecart_type DESC
        LIMIT 5
        """

        result = self.spark.sql(query)
        result.show(truncate=False)
        self._write_csv(result, "top5_variabilite_vibration")
        return result

    # ==================================================================
    # ANALYSE 4
    # ==================================================================
    def analyse_evolution_pression(self):
        logger.info("🔵 Analyse pression horaire")

        query = """
        SELECT
            site,
            HOUR(timestamp) AS heure,
            ROUND(AVG(value), 2) AS pression_moyenne,
            COUNT(*) AS nombre_mesures
        FROM sensor_data
        WHERE type='pressure'
        GROUP BY site, HOUR(timestamp)
        ORDER BY site, heure
        """

        result = self.spark.sql(query)
        result.show(truncate=False)
        self._write_csv(result, "evolution_pression_horaire")
        return result

    # ==================================================================
    def run_all_analyses(self):
        self.analyse_temperature_moyenne()
        self.analyse_alertes_critiques()
        self.analyse_variabilite_vibration()
        self.analyse_evolution_pression()
        logger.info("✅ Toutes les analyses terminées")

    def stop(self):
        self.spark.stop()
        logger.info("🛑 Spark arrêté proprement")


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

    def export_for_powerbi(self):
        logger.info("📊 Export des données pour Power BI")

        query = """
        SELECT
            sensor_id,
            type,
            value,
            unit,
            site,
            machine,
            timestamp,
            year,
            month,
            day,
            hour,
            is_alert
        FROM sensor_data
        ORDER BY timestamp
        """

        df_export = self.spark.sql(query)

        output_path = os.path.join(REPORTS_PATH, "powerbi_export")

        (
            df_export
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(output_path)
        )

        logger.info(f"💾 Export Power BI généré: {output_path}")



def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--analyse",
        choices=["all", "temperature", "alertes", "vibration", "pression", "export"],
        default="all"
    )

    args = parser.parse_args()
    analytics = LakehouseAnalytics()

    try:
        if args.analyse == "all":
            analytics.run_all_analyses()
        elif args.analyse == "temperature":
            analytics.analyse_temperature_moyenne()
        elif args.analyse == "alertes":
            analytics.analyse_alertes_critiques()
        elif args.analyse == "vibration":
            analytics.analyse_variabilite_vibration()
        elif args.analyse == "pression":
            analytics.analyse_evolution_pression()
        elif args.analyse == "export":
            analytics.export_for_powerbi()
    finally:
        analytics.stop()




if __name__ == "__main__":
    main()
