#!/usr/bin/env python3
"""
Pipeline d'intégration simplifié (sans Spark) - InduSense
================================================================================
Version légère pour démonstration et tests locaux
Pour la production, utilisez lakehouse_pipeline.py avec Spark + Delta Lake
================================================================================
"""

import json
import os
import glob
import pandas as pd
from datetime import datetime
import logging
import shutil

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chemins
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_PATH, "data_lake/raw")
WAREHOUSE_PATH = os.path.join(BASE_PATH, "data_lake/warehouse/sensors")
REPORTS_PATH = os.path.join(BASE_PATH, "reports")

# Seuils d'alerte
ALERT_THRESHOLDS = {
    "temperature": {"min": 0, "max": 85},
    "vibration": {"min": 0, "max": 7.0},
    "pressure": {"min": 0.5, "max": 6.0}
}

SENSOR_TYPES = ["temperature", "vibration", "pressure"]


class SimplePipeline:
    """Pipeline simplifié utilisant Pandas"""
    
    def __init__(self):
        logger.info("Initialisation du pipeline simplifié...")
        os.makedirs(WAREHOUSE_PATH, exist_ok=True)
        os.makedirs(REPORTS_PATH, exist_ok=True)
        logger.info("Pipeline initialisé")
    
    def load_json_files(self, sensor_type):
        """Charge tous les fichiers JSON d'un type de capteur"""
        path = os.path.join(RAW_PATH, sensor_type, "*.json")
        files = glob.glob(path)
        
        if not files:
            return pd.DataFrame()
        
        data = []
        for file in files:
            try:
                with open(file, 'r') as f:
                    data.append(json.load(f))
            except Exception as e:
                logger.warning(f"Erreur lecture {file}: {e}")
        
        return pd.DataFrame(data)
    
    def validate_data(self, df):
        """Valide les données"""
        if df.empty:
            return df
        
        initial_count = len(df)
        
        # Filtrer les données invalides
        df = df.dropna(subset=['sensor_id', 'type', 'value', 'site', 'machine', 'timestamp'])
        df = df[df['type'].isin(SENSOR_TYPES)]
        df = df[df['value'] >= 0]
        
        valid_count = len(df)
        logger.info(f" Validation: {valid_count}/{initial_count} enregistrements valides")
        
        return df
    
    def transform_data(self, df):
        """Transforme les données"""
        if df.empty:
            return df
        
        # Convertir timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ajouter colonnes de partitionnement
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['hour'] = df['timestamp'].dt.hour
        
        # Ajouter flag d'alerte
        def check_alert(row):
            thresholds = ALERT_THRESHOLDS.get(row['type'], {})
            if thresholds:
                return row['value'] < thresholds['min'] or row['value'] > thresholds['max']
            return False
        
        df['is_alert'] = df.apply(check_alert, axis=1)
        df['ingestion_timestamp'] = datetime.now()
        
        return df
    
    def save_to_warehouse(self, df):
        """Sauvegarde les données (CSV pour démo, Parquet/Delta en production)"""
        if df.empty:
            return
        
        # Sauvegarder en CSV (pour démo sans pyarrow)
        # En production avec Spark: utiliser Delta Lake / Parquet
        output_file = os.path.join(WAREHOUSE_PATH, "sensors_data.csv")
        df.to_csv(output_file, index=False)
        
        logger.info(f" {len(df)} enregistrements sauvegardés dans {output_file}")
        
        return output_file
    
    def run_pipeline(self):
        """Exécute le pipeline complet"""
        logger.info("=" * 60)
        logger.info(" Démarrage du pipeline simplifié")
        logger.info("=" * 60)
        
        all_data = []
        
        for sensor_type in SENSOR_TYPES:
            logger.info(f"\nTraitement des données {sensor_type}...")
            df = self.load_json_files(sensor_type)
            if not df.empty:
                all_data.append(df)
                logger.info(f"   Chargé: {len(df)} mesures")
        
        if not all_data:
            logger.warning("Aucune donnée trouvée")
            return None
        
        # Combiner toutes les données
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"\nTotal mesures brutes: {len(combined_df)}")
        
        # Pipeline: Validation → Transformation → Sauvegarde
        df_valid = self.validate_data(combined_df)
        df_transformed = self.transform_data(df_valid)
        self.save_to_warehouse(df_transformed)
        
        # Statistiques
        logger.info("\n" + "=" * 60)
        logger.info("Statistiques du pipeline")
        logger.info("=" * 60)
        
        stats = df_transformed.groupby('type').agg({
            'value': ['count', 'mean', 'std', 'min', 'max']
        }).round(2)
        print(stats)
        
        alert_count = df_transformed['is_alert'].sum()
        logger.info(f"\nAlertes détectées: {alert_count}")
        
        return df_transformed
    
    def run_analyses(self, df=None):
        """Exécute toutes les analyses"""
        
        if df is None:
            # Charger depuis le warehouse
            csv_file = os.path.join(WAREHOUSE_PATH, "sensors_data.csv")
            if not os.path.exists(csv_file):
                logger.error("Aucune donnée dans le warehouse. Exécutez d'abord le pipeline.")
                return
            df = pd.read_csv(csv_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info("\n" + "=" * 70)
        logger.info("ANALYSES DÉCISIONNELLES")
        logger.info("=" * 70)
        
        # Analyse 1: Température moyenne par site et machine
        logger.info("\nANALYSE 1: Température moyenne par site et machine")
        logger.info("-" * 50)
        temp_df = df[df['type'] == 'temperature']
        temp_analysis = temp_df.groupby(['site', 'machine']).agg({
            'value': ['mean', 'min', 'max', 'count']
        }).round(2)
        temp_analysis.columns = ['temp_moyenne', 'temp_min', 'temp_max', 'nb_mesures']
        print(temp_analysis)
        temp_analysis.to_csv(os.path.join(REPORTS_PATH, "temperature_moyenne.csv"))
        
        # Analyse 2: Alertes critiques par type
        logger.info("\nANALYSE 2: Alertes critiques par type de capteur")
        logger.info("-" * 50)
        alert_analysis = df.groupby('type').agg({
            'is_alert': ['sum', 'count']
        })
        alert_analysis.columns = ['alertes', 'total']
        alert_analysis['pourcentage'] = (alert_analysis['alertes'] / alert_analysis['total'] * 100).round(2)
        print(alert_analysis)
        alert_analysis.to_csv(os.path.join(REPORTS_PATH, "alertes_critiques.csv"))
        
        # Analyse 3: Top 5 variabilité vibration
        logger.info("\n ANALYSE 3: Top 5 machines - Variabilité de vibration")
        logger.info("-" * 50)
        vib_df = df[df['type'] == 'vibration']
        vib_variability = vib_df.groupby(['machine', 'site']).agg({
            'value': ['std', 'mean', 'count']
        }).round(3)
        vib_variability.columns = ['ecart_type', 'moyenne', 'nb_mesures']
        vib_variability = vib_variability.sort_values('ecart_type', ascending=False).head(5)
        print(vib_variability)
        vib_variability.to_csv(os.path.join(REPORTS_PATH, "top5_variabilite_vibration.csv"))
        
        # Analyse 4: Évolution horaire pression
        logger.info("\n ANALYSE 4: Évolution horaire de la pression par site")
        logger.info("-" * 50)
        press_df = df[df['type'] == 'pressure']
        press_hourly = press_df.groupby(['site', 'hour']).agg({
            'value': ['mean', 'std', 'count']
        }).round(2)
        press_hourly.columns = ['pression_moyenne', 'ecart_type', 'nb_mesures']
        print(press_hourly)
        press_hourly.to_csv(os.path.join(REPORTS_PATH, "evolution_pression_horaire.csv"))
        
        # Export Power BI
        logger.info("\n Export pour Power BI...")
        df.to_csv(os.path.join(REPORTS_PATH, "powerbi_export.csv"), index=False)
        
        logger.info("\n" + "=" * 70)
        logger.info("ANALYSES TERMINÉES")
        logger.info(f"Rapports générés dans: {REPORTS_PATH}")
        logger.info("=" * 70)


def main():
    """Point d'entrée principal"""
    pipeline = SimplePipeline()
    
    # Exécuter le pipeline
    df = pipeline.run_pipeline()
    
    if df is not None:
        # Exécuter les analyses
        pipeline.run_analyses(df)


if __name__ == "__main__":
    main()
