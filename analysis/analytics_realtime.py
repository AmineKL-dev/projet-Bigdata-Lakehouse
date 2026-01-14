#!/usr/bin/env python3
"""
Analytics Real-Time pour InduSense
Analyse continue des données du Data Lakehouse avec export pour Power BI
"""

import os
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, stddev, min as spark_min, max as spark_max, hour, sum as spark_sum
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
POWERBI_PATH = os.path.join(REPORTS_PATH, "powerbi_realtime")

os.makedirs(REPORTS_PATH, exist_ok=True)
os.makedirs(POWERBI_PATH, exist_ok=True)
os.makedirs("C:/stmp", exist_ok=True)

# ------------------------------------------------------------------
# ALERT THRESHOLDS
# ------------------------------------------------------------------
ALERT_THRESHOLDS = {
    "temperature": {"min": 0, "max": 85},
    "vibration": {"min": 0, "max": 7.0},
    "pressure": {"min": 0.5, "max": 6.0}
}


class RealtimeAnalytics:

    def __init__(self):
        logger.info("🚀 Initialisation Spark Analytics Real-Time...")

        builder = (
            SparkSession.builder
            .appName("InduSense_Realtime_Analytics")
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.local.dir", "C:/stmp")
            .config("spark.driver.extraJavaOptions", "-Dlog4j2.formatMsgNoLookups=true")
            .config("spark.executor.extraJavaOptions", "-Dlog4j2.formatMsgNoLookups=true")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "2g")
        )

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        logger.info("✅ Environnement prêt")

    # ------------------------------------------------------------------
    # CHARGEMENT DES DONNÉES (rafraîchi à chaque appel)
    # ------------------------------------------------------------------
    def _load_data(self):
        """Charge les données depuis Delta Lake"""
        if not os.path.exists(WAREHOUSE_PATH):
            raise FileNotFoundError(f"❌ Warehouse introuvable: {WAREHOUSE_PATH}")

        self.df = self.spark.read.format("delta").load(WAREHOUSE_PATH)
        self.df.createOrReplaceTempView("sensor_data")
        
        return self.df.count()

    # ------------------------------------------------------------------
    # EXPORT CSV OPTIMISÉ (1 SEUL FICHIER)
    # ------------------------------------------------------------------
    def _export_single_csv(self, df, filename):
        """Exporte un DataFrame en UN SEUL fichier CSV"""
        temp_path = os.path.join(POWERBI_PATH, f"_{filename}_temp")
        final_path = os.path.join(POWERBI_PATH, f"{filename}.csv")
        
        # Écrire dans un dossier temporaire
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(temp_path)
        )
        
        # Trouver le fichier CSV généré (part-00000-*.csv)
        import glob
        csv_files = glob.glob(os.path.join(temp_path, "part-*.csv"))
        
        if csv_files:
            # Déplacer vers le fichier final
            import shutil
            shutil.move(csv_files[0], final_path)
            
            # Nettoyer le dossier temporaire
            shutil.rmtree(temp_path)
            
            logger.info(f"✅ Exporté: {final_path}")
        else:
            logger.error(f"❌ Erreur export: aucun fichier CSV généré")

    # ==================================================================
    # 1. DONNÉES BRUTES (pour visualisation Power BI)
    # ==================================================================
    def export_raw_data(self):
        """Exporte toutes les données brutes pour Power BI"""
        logger.info("📊 Export données brutes...")
        
        count = self._load_data()
        
        if count == 0:
            logger.warning("⚠️  Aucune donnée à exporter")
            return
        
        # Toutes les colonnes nécessaires
        df_export = self.spark.sql("""
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
                is_alert,
                ingestion_timestamp
            FROM sensor_data
            ORDER BY timestamp DESC
        """)
        
        self._export_single_csv(df_export, "raw_data")
        logger.info(f"   📈 {count} enregistrements exportés")

    # ==================================================================
    # 2. STATISTIQUES PAR TYPE DE CAPTEUR
    # ==================================================================
    def export_stats_by_type(self):
        """Statistiques agrégées par type de capteur"""
        logger.info("📊 Export stats par type...")
        
        self._load_data()
        
        stats = self.spark.sql("""
            SELECT 
                type,
                COUNT(*) as total_mesures,
                ROUND(AVG(value), 2) as valeur_moyenne,
                ROUND(MIN(value), 2) as valeur_min,
                ROUND(MAX(value), 2) as valeur_max,
                ROUND(STDDEV(value), 2) as ecart_type,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as nombre_alertes,
                MAX(timestamp) as derniere_mesure
            FROM sensor_data
            GROUP BY type
            ORDER BY type
        """)
        
        self._export_single_csv(stats, "stats_by_type")

    # ==================================================================
    # 3. STATISTIQUES PAR SITE
    # ==================================================================
    def export_stats_by_site(self):
        """Statistiques par site industriel"""
        logger.info("📊 Export stats par site...")
        
        self._load_data()
        
        stats = self.spark.sql("""
            SELECT 
                site,
                type,
                COUNT(*) as total_mesures,
                ROUND(AVG(value), 2) as valeur_moyenne,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alertes,
                MAX(timestamp) as derniere_mesure
            FROM sensor_data
            GROUP BY site, type
            ORDER BY site, type
        """)
        
        self._export_single_csv(stats, "stats_by_site")

    # ==================================================================
    # 4. STATISTIQUES PAR MACHINE
    # ==================================================================
    def export_stats_by_machine(self):
        """Statistiques par machine"""
        logger.info("📊 Export stats par machine...")
        
        self._load_data()
        
        stats = self.spark.sql("""
            SELECT 
                site,
                machine,
                type,
                COUNT(*) as total_mesures,
                ROUND(AVG(value), 2) as valeur_moyenne,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alertes,
                MAX(timestamp) as derniere_mesure
            FROM sensor_data
            GROUP BY site, machine, type
            ORDER BY site, machine, type
        """)
        
        self._export_single_csv(stats, "stats_by_machine")

    # ==================================================================
    # 5. ÉVOLUTION HORAIRE
    # ==================================================================
    def export_hourly_trends(self):
        """Évolution des mesures par heure"""
        logger.info("📊 Export tendances horaires...")
        
        self._load_data()
        
        trends = self.spark.sql("""
            SELECT 
                date_format(timestamp, 'yyyy-MM-dd') as date,
                HOUR(timestamp) as heure,
                type,
                COUNT(*) as nombre_mesures,
                ROUND(AVG(value), 2) as valeur_moyenne,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alertes
            FROM sensor_data
            GROUP BY date_format(timestamp, 'yyyy-MM-dd'), HOUR(timestamp), type
            ORDER BY date, heure, type
        """)
        
        self._export_single_csv(trends, "hourly_trends")

    # ==================================================================
    # 6. ALERTES CRITIQUES
    # ==================================================================
    def export_critical_alerts(self):
        """Export uniquement les alertes critiques"""
        logger.info("📊 Export alertes critiques...")
        
        self._load_data()
        
        alerts = self.spark.sql("""
            SELECT 
                sensor_id,
                type,
                value,
                unit,
                site,
                machine,
                timestamp,
                ingestion_timestamp
            FROM sensor_data
            WHERE is_alert = true
            ORDER BY timestamp DESC
        """)
        
        alert_count = alerts.count()
        
        if alert_count > 0:
            self._export_single_csv(alerts, "critical_alerts")
            logger.info(f"   🚨 {alert_count} alertes exportées")
        else:
            logger.info("   ✅ Aucune alerte critique")

    # ==================================================================
    # 7. DASHBOARD SUMMARY (métriques clés pour Power BI)
    # ==================================================================
    def export_dashboard_summary(self):
        """Métriques clés pour le dashboard Power BI"""
        logger.info("📊 Export résumé dashboard...")
        
        count = self._load_data()
        
        summary = self.spark.sql(f"""
            SELECT
                {count} as total_mesures,
                COUNT(DISTINCT site) as nombre_sites,
                COUNT(DISTINCT machine) as nombre_machines,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as total_alertes,
                ROUND(SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taux_alerte_pct,
                MAX(timestamp) as derniere_mise_a_jour,
                NOW() as export_timestamp
            FROM sensor_data
        """)
        
        self._export_single_csv(summary, "dashboard_summary")

    # ==================================================================
    # EXPORT COMPLET (tous les fichiers)
    # ==================================================================
    def export_all(self):
        """Exporte tous les fichiers pour Power BI"""
        logger.info("=" * 60)
        logger.info("📊 EXPORT COMPLET POUR POWER BI")
        logger.info("=" * 60)
        
        start = time.time()
        
        try:
            self.export_raw_data()
            self.export_stats_by_type()
            self.export_stats_by_site()
            self.export_stats_by_machine()
            self.export_hourly_trends()
            self.export_critical_alerts()
            self.export_dashboard_summary()
            
            duration = time.time() - start
            
            logger.info("=" * 60)
            logger.info(f"✅ Export terminé en {duration:.2f}s")
            logger.info(f"📁 Fichiers disponibles dans: {POWERBI_PATH}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'export: {e}")
            import traceback
            traceback.print_exc()

    # ==================================================================
    # MODE MONITORING CONSOLE (affichage temps réel)
    # ==================================================================
    def monitor_console(self, refresh_interval=10):
        """Affiche les statistiques en temps réel dans la console"""
        logger.info("=" * 60)
        logger.info("📊 MODE MONITORING CONSOLE")
        logger.info(f"⏱️  Rafraîchissement: {refresh_interval}s")
        logger.info("=" * 60)
        
        try:
            while True:
                count = self._load_data()
                
                if count == 0:
                    logger.warning("⚠️  Aucune donnée disponible")
                    time.sleep(refresh_interval)
                    continue
                
                # Stats globales
                stats = self.spark.sql("""
                    SELECT 
                        type,
                        COUNT(*) as count,
                        ROUND(AVG(value), 2) as avg_value,
                        ROUND(MIN(value), 2) as min_value,
                        ROUND(MAX(value), 2) as max_value,
                        SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alerts
                    FROM sensor_data
                    GROUP BY type
                    ORDER BY type
                """)
                
                # Dernières mesures
                recent = self.spark.sql("""
                    SELECT 
                        type, 
                        site, 
                        machine, 
                        ROUND(value, 2) as value,
                        CASE WHEN is_alert THEN '🚨' ELSE '✅' END as status,
                        timestamp
                    FROM sensor_data
                    ORDER BY ingestion_timestamp DESC
                    LIMIT 10
                """)
                
                # Affichage
                os.system('cls' if os.name == 'nt' else 'clear')
                print("=" * 80)
                print(f"📊 MONITORING TEMPS RÉEL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"📈 Total mesures: {count}")
                print("=" * 80)
                
                print("\n📈 Statistiques par type de capteur:")
                stats.show(truncate=False)
                
                print("\n🕐 Dernières mesures:")
                recent.show(truncate=False)
                
                print(f"\n⏱️  Prochain rafraîchissement dans {refresh_interval}s... (Ctrl+C pour arrêter)")
                
                time.sleep(refresh_interval)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Monitoring arrêté par l'utilisateur")

    # ==================================================================
    # MODE AUTO-REFRESH (export automatique périodique)
    # ==================================================================
    def auto_refresh_export(self, interval=30):
        """Export automatique périodique pour Power BI"""
        logger.info("=" * 60)
        logger.info("🔄 MODE AUTO-REFRESH POUR POWER BI")
        logger.info(f"⏱️  Intervalle: {interval}s")
        logger.info(f"📁 Destination: {POWERBI_PATH}")
        logger.info("=" * 60)
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                logger.info(f"\n🔄 Export #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                self.export_all()
                
                logger.info(f"⏱️  Prochain export dans {interval}s... (Ctrl+C pour arrêter)\n")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Auto-refresh arrêté par l'utilisateur")

    # ==================================================================
    def stop(self):
        """Arrête proprement la session Spark"""
        self.spark.stop()
        logger.info("🛑 Spark arrêté proprement")


# ==================================================================
# MAIN
# ==================================================================
def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Analytics Real-Time pour InduSense - Export Power BI"
    )
    
    parser.add_argument(
        "--mode",
        choices=["export", "monitor", "auto-refresh"],
        default="export",
        help="Mode d'exécution"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Intervalle de rafraîchissement (secondes) pour auto-refresh"
    )
    
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=10,
        help="Intervalle d'affichage (secondes) pour le monitoring console"
    )

    args = parser.parse_args()
    analytics = RealtimeAnalytics()

    try:
        if args.mode == "export":
            # Export unique
            analytics.export_all()
            
        elif args.mode == "monitor":
            # Monitoring console en temps réel
            analytics.monitor_console(refresh_interval=args.monitor_interval)
            
        elif args.mode == "auto-refresh":
            # Export automatique périodique pour Power BI
            analytics.auto_refresh_export(interval=args.interval)
            
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        analytics.stop()


if __name__ == "__main__":
    main()
