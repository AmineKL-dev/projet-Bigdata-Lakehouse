#!/usr/bin/env python3
"""
Pipeline d'intégration Data Lakehouse - InduSense
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
import shutil
import time
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    to_timestamp, lit, when, count, avg,
    current_timestamp, lower, trim, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_PATH, "data_lake/raw")
WAREHOUSE_PATH = os.path.join(BASE_PATH, "data_lake/warehouse/sensors")
CHECKPOINT_PATH = os.path.join(BASE_PATH, "checkpoints")
PROCESSED_PATH = os.path.join(BASE_PATH, "data_lake/processed")

SENSOR_TYPES = ["temperature", "vibration", "pressure"]

ALERT_THRESHOLDS = {
    "temperature": {"min": 0, "max": 85},
    "vibration": {"min": 0, "max": 7.0},
    "pressure": {"min": 0.5, "max": 6.0}
}

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
    StructField("unit", StringType(), nullable=True),
    StructField("site", StringType(), nullable=True),
    StructField("machine", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
])


class LakehousePipeline:

    
    import uuid
    import time
    import shutil
    import stat

    def _write_log4j2_silence_file(self, path: str) -> None:
        """
        Crée un log4j2.properties qui supprime les WARN/ERROR de nettoyage temp (SparkEnv/ShutdownHookManager)
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)

        content = """\
            status = error
            name = SparkSilence

            appender.console.type = Console
            appender.console.name = console
            appender.console.target = SYSTEM_ERR
            appender.console.layout.type = PatternLayout
            appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

            rootLogger.level = ERROR
            rootLogger.appenderRefs = stdout
            rootLogger.appenderRef.stdout.ref = console

            logger.sparkEnv.name = org.apache.spark.SparkEnv
            logger.sparkEnv.level = ERROR

            logger.shutdown.name = org.apache.spark.util.ShutdownHookManager
            logger.shutdown.level = ERROR

            logger.sparkFileUtils.name = org.apache.spark.util.SparkFileUtils
            logger.sparkFileUtils.level = ERROR
            """
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)


    def __init__(self):
        logger.info("Initialisation du pipeline Lakehouse...")

        os.makedirs(WAREHOUSE_PATH, exist_ok=True)
        os.makedirs(CHECKPOINT_PATH, exist_ok=True)
        os.makedirs(PROCESSED_PATH, exist_ok=True)

        # Dossier temp root (chemin court, sans espaces)
        tmp_root = r"C:\stmp"
        os.makedirs(tmp_root, exist_ok=True)

        # Temp unique par run
        tmp_dir = os.path.join(tmp_root, f"run_{uuid.uuid4().hex}")
        os.makedirs(tmp_dir, exist_ok=True)
        self._tmp_dir = tmp_dir

        # log4j2 pour masquer les erreurs de delete temp
        log4j_path = os.path.join(tmp_root, "conf", "log4j2.properties")
        if not os.path.exists(log4j_path):
            self._write_log4j2_silence_file(log4j_path)

        log4j_opt = f"-Dlog4j.configurationFile=file:{log4j_path}"

        builder = (
            SparkSession.builder
            .appName("InduSense_Lakehouse_Pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.instances", "1")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.maxResultSize", "1g") 
            # Temp Spark unique
            .config("spark.local.dir", tmp_dir)

            # Parser timestamp
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

            # Masquer les WARN/ERROR de delete temp
            .config("spark.driver.extraJavaOptions", log4j_opt)
            .config("spark.executor.extraJavaOptions", log4j_opt)
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Tu peux mettre ERROR si tu veux encore moins de bruit
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.sparkContext.setLogLevel("FATAL")


        logger.info(" Pipeline initialisé avec succès")



    # -------------------------------------------------------------------------
    # READ (BATCH)
    # -------------------------------------------------------------------------
    def process_sensor_type_batch(self, sensor_type: str) -> DataFrame | None:
        raw_path = os.path.join(RAW_PATH, sensor_type)
        if not os.path.exists(raw_path):
            logger.warning(f"⚠️  Répertoire non trouvé: {raw_path}")
            return None

        json_files = [f for f in os.listdir(raw_path) if f.lower().endswith(".json")]
        if not json_files:
            logger.info(f"   Aucun fichier JSON trouvé dans {raw_path}")
            return None

        logger.info(f" Traitement de {len(json_files)} fichiers {sensor_type}...")
        logger.info(f"   Path: {os.path.join(raw_path, '*.json')}")

        df = (
            self.spark.read
            .schema(SENSOR_SCHEMA)
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .json(os.path.join(raw_path, "*.json"))
        )

        logger.info(f"    Lignes lues (avant nettoyage): {df.count()}")

        df = (
            df.withColumn("type", lower(trim(col("type"))))
              .withColumn("sensor_id", trim(col("sensor_id")))
              .withColumn("site", trim(col("site")))
              .withColumn("machine", trim(col("machine")))
              .withColumn("timestamp", trim(col("timestamp")))
              .withColumn("value", col("value").cast("double"))
        )

        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"     Lignes corrompues détectées: {corrupt_count}")
                df.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record").show(5, truncate=False)
        else:
            logger.info("   (info) Colonne _corrupt_record absente (OK).")

        return df

    # -------------------------------------------------------------------------
    # VALIDATE
    # -------------------------------------------------------------------------
    def validate_data(self, df: DataFrame) -> DataFrame:
        logger.info("🔍 Validation des données...")

        base_filter = (
            col("sensor_id").isNotNull() &
            col("type").isNotNull() &
            col("value").isNotNull() &
            col("site").isNotNull() &
            col("machine").isNotNull() &
            col("timestamp").isNotNull() &
            col("type").isin(SENSOR_TYPES) &
            (col("value") >= 0)
        )

        if "_corrupt_record" in df.columns:
            base_filter = base_filter & col("_corrupt_record").isNull()

        df_valid = df.filter(base_filter)

        # ✅ EN STREAMING : Afficher un échantillon AVANT et APRÈS validation
        if df.isStreaming:
            logger.info("📋 Échantillon AVANT validation (take 3):")
            try:
                # Note: take() fonctionne en streaming mais pas count()
                sample = df.take(3)
                for row in sample:
                    logger.info(f"   {row}")
            except Exception as e:
                logger.warning(f"⚠️  Impossible d'afficher l'échantillon: {e}")
            
            logger.info("📋 Échantillon APRÈS validation (take 3):")
            try:
                sample_valid = df_valid.take(3)
                for row in sample_valid:
                    logger.info(f"   {row}")
                if not sample_valid:
                    logger.error("❌ AUCUNE LIGNE VALIDE après validation!")
            except Exception as e:
                logger.warning(f"⚠️  Impossible d'afficher l'échantillon: {e}")
        else:
            initial_count = df.count()
            valid_count = df_valid.count()
            rejected = initial_count - valid_count
            if rejected > 0:
                logger.warning(f"⚠️  {rejected} lignes rejetées (invalides)")
            logger.info(f"✅ Validation: {valid_count}/{initial_count} valides")

        return df_valid


    # -------------------------------------------------------------------------
    # TRANSFORM (FIX TIMESTAMP)
    # -------------------------------------------------------------------------
    def transform_data(self, df: DataFrame) -> DataFrame:
        logger.info("🔍 Transformation des données...")

        # ✅ Vérifier AVANT transformation
        if df.isStreaming:
            logger.info("📋 Échantillon AVANT transformation (take 3):")
            try:
                sample = df.take(3)
                for row in sample:
                    logger.info(f"   Type timestamp: {type(row['timestamp'])}, Valeur: {row['timestamp']}")
                if not sample:
                    logger.error("❌ DataFrame VIDE avant transformation!")
            except Exception as e:
                logger.warning(f"⚠️  Erreur échantillon: {e}")

        # ... votre code de transformation existant ...

        df2 = df.withColumn("ts_raw", trim(col("timestamp")))
        df2 = df2.withColumn("ts_raw", regexp_replace(col("ts_raw"), ",", "."))

        df2 = df2.withColumn("ts_ms", regexp_replace(col("ts_raw"), r"(\.\d{3})\d+", "$1"))
        df2 = df2.withColumn(
            "ts_clean",
            regexp_replace(col("ts_ms"), r"(Z|[+-]\d{2}:?\d{2})$", "")
        )

        ts_t_ms  = to_timestamp(col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        ts_sp_ms = to_timestamp(col("ts_clean"), "yyyy-MM-dd HH:mm:ss.SSS")
        ts_t     = to_timestamp(col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss")
        ts_sp    = to_timestamp(col("ts_clean"), "yyyy-MM-dd HH:mm:ss")
        ts_fb    = to_timestamp(col("ts_clean"))

        df_t = df2.withColumn(
            "timestamp_ts",
            when(ts_t_ms.isNotNull(), ts_t_ms)
            .when(ts_sp_ms.isNotNull(), ts_sp_ms)
            .when(ts_t.isNotNull(), ts_t)
            .when(ts_sp.isNotNull(), ts_sp)
            .otherwise(ts_fb)
        )

        # ✅ Vérifier APRÈS parsing timestamp
        if df.isStreaming:
            logger.info("📋 Vérification parsing timestamp (take 3):")
            try:
                sample = df_t.take(3)
                for row in sample:
                    logger.info(f"   timestamp_ts: {row['timestamp_ts']}")
                if not sample:
                    logger.error("❌ Toutes les lignes perdues après parsing timestamp!")
            except Exception as e:
                logger.warning(f"⚠️  Erreur: {e}")

        df_t = df_t.filter(col("timestamp_ts").isNotNull())

        # ✅ Vérifier APRÈS filter
        if df.isStreaming:
            logger.info("📋 Après filter timestamp (take 3):")
            try:
                sample = df_t.take(3)
                if not sample:
                    logger.error("❌ TOUTES les lignes rejetées par filter timestamp!")
                else:
                    logger.info(f"✅ {len(sample)} lignes survivent après filter")
            except Exception as e:
                logger.warning(f"⚠️  Erreur: {e}")

        df_t = (
            df_t.drop("timestamp", "ts_raw", "ts_ms", "ts_clean")
                .withColumnRenamed("timestamp_ts", "timestamp")
        )

        df_t = (
            df_t
            .withColumn("year", year("timestamp"))
            .withColumn("month", month("timestamp"))
            .withColumn("day", dayofmonth("timestamp"))
            .withColumn("hour", hour("timestamp"))
        )

        df_t = df_t.withColumn(
            "is_alert",
            when(
                (col("type") == "temperature") &
                ((col("value") < ALERT_THRESHOLDS["temperature"]["min"]) |
                (col("value") > ALERT_THRESHOLDS["temperature"]["max"])),
                True
            ).when(
                (col("type") == "vibration") &
                ((col("value") < ALERT_THRESHOLDS["vibration"]["min"]) |
                (col("value") > ALERT_THRESHOLDS["vibration"]["max"])),
                True
            ).when(
                (col("type") == "pressure") &
                ((col("value") < ALERT_THRESHOLDS["pressure"]["min"]) |
                (col("value") > ALERT_THRESHOLDS["pressure"]["max"])),
                True
            ).otherwise(False)
        )

        df_t = df_t.withColumn("ingestion_timestamp", current_timestamp())

        logger.info("✅ Transformation terminée")
        return df_t


    # -------------------------------------------------------------------------
    # WRITE
    # -------------------------------------------------------------------------
    def save_to_delta(self, df: DataFrame, mode: str = "append") -> None:
        logger.info(f" Sauvegarde en Delta Lake ({mode})...")
        logger.info(f"   Chemin: {WAREHOUSE_PATH}")

        record_count = df.count()
        if record_count == 0:
            logger.warning("  DataFrame vide, rien à écrire en Delta.")
            return

        (
            df.write
            .format("delta")
            .mode(mode)
            .partitionBy("site", "type", "year", "month", "day")
            .save(WAREHOUSE_PATH)
        )

        logger.info(f"{record_count} enregistrements sauvegardés en Delta Lake")

    # -------------------------------------------------------------------------
    # BATCH
    # -------------------------------------------------------------------------
    def run_batch_pipeline(self, overwrite: bool = False) -> None:
        
        logger.info("Démarrage du pipeline Lakehouse (Mode Batch)")
        logger.info("=" * 60)

        start = datetime.now()

        all_dfs: list[DataFrame] = []
        for sensor_type in SENSOR_TYPES:
            df = self.process_sensor_type_batch(sensor_type)
            if df is not None:
                all_dfs.append(df)

        if not all_dfs:
            logger.warning("⚠️  Aucune donnée à traiter")
            return

        logger.info(" Fusion des données de tous les capteurs...")
        combined = all_dfs[0]
        for df in all_dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        logger.info(f" Total mesures brutes: {combined.count()}")

        df_valid = self.validate_data(combined)
        df_transformed = self.transform_data(df_valid)

        write_mode = "overwrite" if overwrite else "append"
        self.save_to_delta(df_transformed, mode=write_mode)

        duration = (datetime.now() - start).total_seconds()
        logger.info("=" * 60)
        logger.info(" Résumé du pipeline")
        logger.info("=" * 60)
        logger.info(f"   Durée totale: {duration:.2f} s")
        logger.info(f"   Enregistrements traités: {df_transformed.count()}")

        alert_count = df_transformed.filter(col("is_alert") == True).count()
        logger.info(f"   Alertes détectées: {alert_count}")

        logger.info("=" * 60)
        logger.info("Pipeline terminé avec succès!")
        logger.info("=" * 60)

    # -------------------------------------------------------------------------
    # STATS
    # -------------------------------------------------------------------------
    def show_warehouse_stats(self) -> None:
        logger.info("=" * 60)
        logger.info("Statistiques du Data Warehouse")
        logger.info("=" * 60)

        if not os.path.exists(WAREHOUSE_PATH):
            logger.warning("Le warehouse n'existe pas encore")
            return

        if not DeltaTable.isDeltaTable(self.spark, WAREHOUSE_PATH):
            logger.warning("Aucun Delta table valide trouvé (pas de _delta_log).")
            return

        df = self.spark.read.format("delta").load(WAREHOUSE_PATH)

        logger.info(f"   Total enregistrements: {df.count()}")

        logger.info("\n   Par type de capteur:")
        df.groupBy("type").count().show(truncate=False)

        logger.info("\n   Par site:")
        df.groupBy("site").count().show(truncate=False)

        alert_count = df.filter(col("is_alert") == True).count()
        logger.info(f"\n  Total alertes: {alert_count}")

    def stop(self) -> None:
        logger.info("Arrêt de la session Spark...")
        try:
            self.spark.stop()
        finally:
            logger.info(" Session Spark arrêtée")

        # Cleanup best-effort (Windows lock friendly)
        tmp_dir = getattr(self, "_tmp_dir", None)
        if tmp_dir and os.path.exists(tmp_dir):

            def onerror(func, path, exc_info):
                try:
                    os.chmod(path, stat.S_IWRITE)
                    func(path)
                except Exception:
                    pass

            for i in range(6):
                try:
                    shutil.rmtree(tmp_dir, onerror=onerror)
                    logger.info(f" Temp supprimé: {tmp_dir}")
                    break
                except Exception:
                    time.sleep(2)
                    if i == 5:
                        logger.warning(f"Temp encore verrouillé (Windows): {tmp_dir}")
    # -------------------------------------------------------------------------
    # REAL-TIME
    # -------------------------------------------------------------------------
    def debug_batch_raw(self, batch_df: DataFrame, batch_id: int):
        """Version DEBUG : affiche les données brutes sans validation"""
        logger.info(f"🟢 Batch RAW {batch_id} reçu")
        
        logger.info("📋 Schema complet:")
        batch_df.printSchema()
        
        logger.info("📋 Toutes les colonnes:")
        logger.info(batch_df.columns)
        
        logger.info("📋 Données (10 premières lignes):")
        batch_df.show(10, truncate=False)
        
        count = batch_df.count()
        logger.info(f"📊 Total lignes: {count}")
        
        if count > 0:
            logger.info("✅ Des données SONT présentes !")
            
            # Vérifier les valeurs nulles
            logger.info("🔍 Valeurs nulles par colonne:")
            for col_name in batch_df.columns:
                null_count = batch_df.filter(col(col_name).isNull()).count()
                logger.info(f"   {col_name}: {null_count} nulls")


    def debug_batch(self, batch_df: DataFrame, batch_id: int):
        """Traite chaque micro-batch et l'écrit en Delta Lake"""
        logger.info(f"🟢 Batch {batch_id} reçu")
        
        count = batch_df.count()
        
        if count == 0:
            logger.warning(f"⚠️  Batch {batch_id} vide")
            return
        
        logger.info(f"📊 Batch {batch_id}: {count} enregistrements")
        
        # Afficher un échantillon
        logger.info("📋 Échantillon (5 lignes):")
        batch_df.select(
            "sensor_id", "type", "value", "site", "machine", "timestamp"
        ).show(5, truncate=False)
        
        # ✅ ÉCRIRE EN DELTA LAKE
        try:
            (
                batch_df.write
                .format("delta")
                .mode("append")
                .option("compression", "snappy")
                .partitionBy("site", "type", "year", "month", "day")
                .save(WAREHOUSE_PATH)
            )
            
            logger.info(f"✅ Batch {batch_id}: {count} enregistrements → Delta Lake")
            
            # Statistiques d'alertes
            alert_count = batch_df.filter(col("is_alert") == True).count()
            if alert_count > 0:
                logger.warning(f"🚨 {alert_count} alertes détectées")
                
        except Exception as e:
            logger.error(f"❌ Erreur écriture batch {batch_id}: {e}")
            import traceback
            traceback.print_exc()




    def run_realtime_pipeline(self):
        logger.info("📡 Démarrage du pipeline Lakehouse (Mode Real-Time)")
        logger.info("=" * 60)

        # Checkpoint UNIQUE à chaque run (important!)
        checkpoint_dir = os.path.join(CHECKPOINT_PATH, f"realtime_{int(time.time())}")
        logger.info(f"📌 Checkpoint: {checkpoint_dir}")

        dfs = []
        for sensor_type in SENSOR_TYPES:
            raw_path = os.path.join(RAW_PATH, sensor_type)
            abs_path = os.path.abspath(raw_path)

            if not os.path.exists(abs_path):
                continue

            logger.info(f"📂 {sensor_type}: {abs_path}")

            df = (
                self.spark.readStream
                .schema(SENSOR_SCHEMA)
                .option("maxFilesPerTrigger", 5)
                .option("multiLine", "true")  # ✅ IMPORTANT
                .option("mode", "PERMISSIVE")
                .json(abs_path)
            )

            dfs.append(df)

        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        # Appliquer validation et transformation
        df_valid = self.validate_data(combined)
        df_transformed = self.transform_data(df_valid)

        checkpoint_dir = os.path.join(CHECKPOINT_PATH, f"realtime_{int(time.time())}")
        
        query = (
            df_transformed.writeStream
            .foreachBatch(self.debug_batch)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("🚀 Pipeline Real-Time lancé")
        query.awaitTermination()

    # def stop(self) -> None:
    #     logger.info(" Arrêt de la session Spark...")
    #     self.spark.stop()
    #     logger.info(" Session Spark arrêtée")
    def run_realtime_pipeline_simple(self):
        """Version SIMPLE pour debug - sans validation ni transformation"""
        logger.info("📡 Pipeline Real-Time SIMPLE (DEBUG)")
        logger.info("=" * 60)

        checkpoint_dir = os.path.join(CHECKPOINT_PATH, f"simple_{int(time.time())}")

        dfs = []
        for sensor_type in SENSOR_TYPES:
            raw_path = os.path.join(RAW_PATH, sensor_type)
            abs_path = os.path.abspath(raw_path)

            if not os.path.exists(abs_path):
                continue

            logger.info(f"📂 {sensor_type}: {abs_path}")

            df = (
                self.spark.readStream
                .schema(SENSOR_SCHEMA)
                .option("maxFilesPerTrigger", 5)
                .json(abs_path)
            )

            dfs.append(df)

        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)

        # ✅ AUCUNE TRANSFORMATION - données brutes directes
        query = (
            combined.writeStream
            .foreachBatch(self.debug_batch_ultra_simple)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("🚀 Pipeline SIMPLE lancé")
        query.awaitTermination()


    def debug_batch_ultra_simple(self, batch_df: DataFrame, batch_id: int):
        """Version ultra simple - juste afficher les données brutes"""
        logger.info(f"🟢 Batch {batch_id}")
        
        logger.info("📋 Schema:")
        batch_df.printSchema()
        
        logger.info("📋 Données (ALL):")
        batch_df.show(100, truncate=False)
        
        count = batch_df.count()
        logger.info(f"📊 COUNT: {count}")
        
        if count > 0:
            logger.info("✅✅✅ DONNÉES PRÉSENTES!")
        else:
            logger.error("❌❌❌ VIDE!")

def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stats", "realtime", "realtime_simple"], default="batch")
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()
    pipeline = LakehousePipeline()

    try:
        if args.mode == "batch":
            pipeline.run_batch_pipeline(overwrite=args.overwrite)
        elif args.mode == "stats":
            pipeline.show_warehouse_stats()   
        elif args.mode == "realtime":
            pipeline.run_realtime_pipeline()
        elif args.mode == "realtime_simple":  # ✅ NOUVEAU
            pipeline.run_realtime_pipeline_simple()
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
