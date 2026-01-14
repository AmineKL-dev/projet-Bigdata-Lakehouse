#!/usr/bin/env python3
"""
Script de diagnostic pour identifier le problème de lecture JSON
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

# Configuration
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_PATH, "data_lake/raw")

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
    StructField("unit", StringType(), nullable=True),
    StructField("site", StringType(), nullable=True),
    StructField("machine", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
])

def test_direct_json_read():
    """Test 1: Lecture directe avec json.load Python"""
    print("=" * 60)
    print("TEST 1: Lecture Python native (json.load)")
    print("=" * 60)
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        sensor_path = os.path.join(RAW_PATH, sensor_type)
        
        if not os.path.exists(sensor_path):
            print(f"❌ {sensor_type}: répertoire inexistant")
            continue
        
        json_files = [f for f in os.listdir(sensor_path) if f.endswith('.json')]
        
        if not json_files:
            print(f"⚠️  {sensor_type}: aucun fichier JSON")
            continue
        
        # Tester le premier fichier
        test_file = os.path.join(sensor_path, json_files[0])
        
        try:
            with open(test_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"✅ {sensor_type}: Lecture OK")
            print(f"   Fichier: {json_files[0]}")
            print(f"   Contenu: {data}")
            print()
            
        except Exception as e:
            print(f"❌ {sensor_type}: ERREUR - {e}")
            print(f"   Fichier: {test_file}")
            print()


def test_spark_read_without_schema():
    """Test 2: Lecture Spark SANS schema (inférence automatique)"""
    print("=" * 60)
    print("TEST 2: Lecture Spark SANS schema (inférence)")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("JSON_Test_NoSchema") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        sensor_path = os.path.join(RAW_PATH, sensor_type)
        
        if not os.path.exists(sensor_path):
            continue
        
        json_files = [f for f in os.listdir(sensor_path) if f.endswith('.json')]
        
        if not json_files:
            continue
        
        try:
            # Lecture SANS schema
            df = spark.read \
                .option("multiLine", "true") \
                .json(sensor_path)
            
            count = df.count()
            print(f"✅ {sensor_type}: {count} lignes lues")
            print(f"   Schema inféré:")
            df.printSchema()
            print(f"   Échantillon:")
            df.show(3, truncate=False)
            print()
            
        except Exception as e:
            print(f"❌ {sensor_type}: ERREUR - {e}")
            print()
    
    spark.stop()


def test_spark_read_with_schema():
    """Test 3: Lecture Spark AVEC schema"""
    print("=" * 60)
    print("TEST 3: Lecture Spark AVEC schema défini")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("JSON_Test_WithSchema") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        sensor_path = os.path.join(RAW_PATH, sensor_type)
        
        if not os.path.exists(sensor_path):
            continue
        
        json_files = [f for f in os.listdir(sensor_path) if f.endswith('.json')]
        
        if not json_files:
            continue
        
        try:
            # Lecture AVEC schema
            df = spark.read \
                .schema(SENSOR_SCHEMA) \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(sensor_path)
            
            count = df.count()
            valid_count = df.filter(col("sensor_id").isNotNull()).count()
            
            print(f"{'✅' if valid_count > 0 else '❌'} {sensor_type}: Total={count}, Valides={valid_count}")
            
            if "_corrupt_record" in df.columns:
                corrupted = df.filter(col("_corrupt_record").isNotNull())
                corrupted_count = corrupted.count()
                
                if corrupted_count > 0:
                    print(f"   ⚠️  {corrupted_count} enregistrements corrompus")
                    print(f"   Exemples:")
                    corrupted.select("_corrupt_record").show(3, truncate=False)
            
            if valid_count > 0:
                print(f"   Échantillon valide:")
                df.filter(col("sensor_id").isNotNull()).show(3, truncate=False)
            else:
                print(f"   Toutes les lignes:")
                df.show(3, truncate=False)
            
            print()
            
        except Exception as e:
            print(f"❌ {sensor_type}: ERREUR - {e}")
            print()
    
    spark.stop()


def test_file_encoding():
    """Test 4: Vérifier l'encodage des fichiers"""
    print("=" * 60)
    print("TEST 4: Vérification encodage fichiers")
    print("=" * 60)
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        sensor_path = os.path.join(RAW_PATH, sensor_type)
        
        if not os.path.exists(sensor_path):
            continue
        
        json_files = [f for f in os.listdir(sensor_path) if f.endswith('.json')]
        
        if not json_files:
            continue
        
        # Tester le premier fichier
        test_file = os.path.join(sensor_path, json_files[0])
        
        try:
            # Lire les premiers octets
            with open(test_file, 'rb') as f:
                first_bytes = f.read(10)
            
            # Vérifier BOM UTF-8
            has_bom = first_bytes.startswith(b'\xef\xbb\xbf')
            
            # Lire le contenu
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            print(f"{'⚠️ ' if has_bom else '✅'} {sensor_type}:")
            print(f"   Fichier: {json_files[0]}")
            print(f"   BOM UTF-8: {'OUI (PROBLÈME!)' if has_bom else 'Non'}")
            print(f"   Taille: {len(content)} caractères")
            print(f"   Premiers caractères: {repr(content[:50])}")
            print()
            
        except Exception as e:
            print(f"❌ {sensor_type}: ERREUR - {e}")
            print()


def main():
    print("\n")
    print("🔍 DIAGNOSTIC COMPLET - Lecture JSON")
    print("\n")
    
    # Test 1: Lecture Python native
    test_direct_json_read()
    
    # Test 2: Spark sans schema
    test_spark_read_without_schema()
    
    # Test 3: Spark avec schema
    test_spark_read_with_schema()
    
    # Test 4: Encodage
    test_file_encoding()
    
    print("=" * 60)
    print("🏁 DIAGNOSTIC TERMINÉ")
    print("=" * 60)


if __name__ == "__main__":
    main()