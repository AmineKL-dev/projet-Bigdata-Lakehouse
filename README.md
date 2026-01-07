# 🏭 InduSense - Big Data Lakehouse

## Mini-Projet: Simulation de flux de données de capteurs industriels

**Module:** Data Warehouse et Big Data Warehouse  
**Université:** Sultan Moulay Slimane - ENSA Khouribga  
**Professeur:** M. Mostafa SAADI

---

## 📋 Table des matières

1. [Contexte du projet](#contexte-du-projet)
2. [Architecture](#architecture)
3. [Installation](#installation)
4. [Structure du projet](#structure-du-projet)
5. [Guide d'utilisation](#guide-dutilisation)
6. [Détail des composants](#détail-des-composants)
7. [Analyses décisionnelles](#analyses-décisionnelles)
8. [Intégration Power BI](#intégration-power-bi)

---

## 🎯 Contexte du projet

La société **InduSense** opère plusieurs sites industriels équipés de capteurs IoT qui collectent des mesures techniques (température, pression, vibration). Ce projet simule cet environnement et construit un **Big Data Lakehouse** basé sur **Apache Spark** et **Delta Lake**.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ARCHITECTURE LAKEHOUSE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐ │
│  │  Capteur Temp    │     │  Capteur Vibr    │     │  Capteur Press   │ │
│  │  (Python)        │     │  (Python)        │     │  (Python)        │ │
│  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘ │
│           │                        │                        │           │
│           ▼                        ▼                        ▼           │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                      DATA LAKE BRUT (Raw Zone)                      ││
│  │  /data_lake/raw/                                                    ││
│  │    ├── temperature/  (fichiers JSON)                                ││
│  │    ├── vibration/    (fichiers JSON)                                ││
│  │    └── pressure/     (fichiers JSON)                                ││
│  └─────────────────────────────────┬───────────────────────────────────┘│
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    PIPELINE D'INTÉGRATION                           ││
│  │  (Apache Spark + Delta Lake)                                        ││
│  │                                                                      ││
│  │  1. Surveillance des répertoires raw                                ││
│  │  2. Validation des données JSON                                     ││
│  │  3. Transformation et enrichissement                                ││
│  │  4. Écriture en format Delta Lake                                   ││
│  └─────────────────────────────────┬───────────────────────────────────┘│
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                      DATA LAKEHOUSE (Delta Lake)                    ││
│  │  /data_lake/warehouse/sensors/                                      ││
│  │                                                                      ││
│  │  Partitionnement: site / type / year / month / day                  ││
│  │  Format: Delta Lake (Parquet + Transaction Log)                     ││
│  └─────────────────────────────────┬───────────────────────────────────┘│
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    ANALYSES DÉCISIONNELLES                          ││
│  │  (Spark SQL)                                                        ││
│  │                                                                      ││
│  │  • Température moyenne par site/machine                             ││
│  │  • Alertes critiques par type de capteur                            ││
│  │  • Top 5 variabilité vibration                                      ││
│  │  • Évolution horaire pression                                       ││
│  └─────────────────────────────────┬───────────────────────────────────┘│
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                         REPORTING                                    ││
│  │  (Power BI / CSV Export)                                            ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Installation

### Prérequis

- Python 3.8+
- Java 8 ou 11 (pour Spark)
- Apache Spark 3.x
- Minimum 4 Go RAM

### Installation des dépendances

```bash
# Cloner ou télécharger le projet
cd indusense_lakehouse

# Créer un environnement virtuel (recommandé)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou: venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### Configuration Java (si nécessaire)

```bash
# Vérifier la version de Java
java -version

# Définir JAVA_HOME si nécessaire
export JAVA_HOME=/path/to/java
```

---

## 📁 Structure du projet

```
indusense_lakehouse/
│
├── simulators/                    # Partie 1: Simulateurs de capteurs
│   ├── temperature_sensor.py      # Génère des mesures de température
│   ├── vibration_sensor.py        # Génère des mesures de vibration
│   ├── pressure_sensor.py         # Génère des mesures de pression
│   └── run_all_simulators.py      # Lance tous les simulateurs
│
├── pipeline/                      # Partie 2: Pipeline d'intégration
│   └── lakehouse_pipeline.py      # Pipeline Spark + Delta Lake
│
├── analysis/                      # Partie 3: Analyses Spark SQL
│   └── spark_analytics.py         # Requêtes analytiques
│
├── data_lake/                     # Stockage des données
│   ├── raw/                       # Zone brute (JSON)
│   │   ├── temperature/
│   │   ├── vibration/
│   │   └── pressure/
│   └── warehouse/                 # Zone Lakehouse (Delta)
│       └── sensors/
│
├── reports/                       # Rapports générés (CSV)
├── checkpoints/                   # Points de contrôle Spark
├── requirements.txt               # Dépendances Python
└── README.md                      # Documentation
```

---

## 🚀 Guide d'utilisation

### Étape 1: Générer les données (Simulateurs)

```bash
cd simulators

# Lancer tous les simulateurs (1000 mesures par capteur)
python run_all_simulators.py

# Ou lancer un simulateur spécifique
python temperature_sensor.py
python vibration_sensor.py
python pressure_sensor.py

# Avec un nombre personnalisé de mesures
python run_all_simulators.py 5000
```

### Étape 2: Exécuter le Pipeline d'intégration

```bash
cd pipeline

# Mode batch (traite tous les fichiers existants)
python lakehouse_pipeline.py --mode batch

# Mode streaming (surveillance continue)
python lakehouse_pipeline.py --mode streaming

# Afficher les statistiques du warehouse
python lakehouse_pipeline.py --mode stats
```

### Étape 3: Lancer les analyses

```bash
cd analysis

# Toutes les analyses
python spark_analytics.py --analyse all

# Analyses individuelles
python spark_analytics.py --analyse temperature
python spark_analytics.py --analyse alertes
python spark_analytics.py --analyse vibration
python spark_analytics.py --analyse pression

# Export pour Power BI
python spark_analytics.py --analyse export
```

---

## 📦 Détail des composants

### 1. Simulateurs de capteurs

Chaque simulateur génère des mesures au format JSON:

```json
{
  "sensor_id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "temperature",
  "value": 45.67,
  "unit": "Celsius",
  "site": "Site_Paris",
  "machine": "Machine_A1",
  "timestamp": "2026-01-08T14:30:45.123456"
}
```

**Configuration des capteurs:**

| Capteur     | Unité   | Plage normale | Seuil critique |
|-------------|---------|---------------|----------------|
| Température | Celsius | 20 - 80       | > 85           |
| Vibration   | mm/s    | 0.5 - 4.5     | > 7.0          |
| Pression    | bar     | 1.0 - 5.0     | > 6.0          |

### 2. Pipeline d'intégration

Le pipeline effectue les opérations suivantes:

1. **Surveillance**: Détecte les nouveaux fichiers JSON
2. **Validation**: Vérifie la structure et la cohérence
3. **Transformation**:
   - Conversion des timestamps
   - Ajout des colonnes de partitionnement (year, month, day, hour)
   - Calcul des flags d'alerte
4. **Stockage**: Écriture en format Delta Lake partitionné

**Partitionnement Delta Lake:**
```
/data_lake/warehouse/sensors/
  └── site=Site_Paris/
      └── type=temperature/
          └── year=2026/
              └── month=1/
                  └── day=8/
                      └── part-00000.parquet
```

### 3. Analyses Spark SQL

Quatre analyses principales sont implémentées:

1. **Température moyenne par site et machine**
2. **Alertes critiques par type de capteur**
3. **Top 5 variabilité de vibration**
4. **Évolution horaire de la pression**

---

## 📊 Analyses décisionnelles

### Analyse 1: Température moyenne

```sql
SELECT 
    site,
    machine,
    date_format(timestamp, 'yyyy-MM-dd') as date,
    ROUND(AVG(value), 2) as temperature_moyenne,
    COUNT(*) as nombre_mesures
FROM sensor_data
WHERE type = 'temperature'
GROUP BY site, machine, date_format(timestamp, 'yyyy-MM-dd')
ORDER BY site, machine, date
```

### Analyse 2: Alertes critiques

```sql
SELECT 
    type as type_capteur,
    COUNT(*) as total_mesures,
    SUM(CASE WHEN is_alert = true THEN 1 ELSE 0 END) as alertes_critiques,
    ROUND(SUM(CASE WHEN is_alert = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pourcentage
FROM sensor_data
GROUP BY type
ORDER BY alertes_critiques DESC
```

### Analyse 3: Variabilité vibration

```sql
SELECT 
    machine,
    site,
    ROUND(STDDEV(value), 3) as ecart_type,
    ROUND(AVG(value), 3) as moyenne,
    COUNT(*) as mesures
FROM sensor_data
WHERE type = 'vibration'
GROUP BY machine, site
ORDER BY ecart_type DESC
LIMIT 5
```

### Analyse 4: Évolution pression

```sql
SELECT 
    site,
    HOUR(timestamp) as heure,
    ROUND(AVG(value), 2) as pression_moyenne
FROM sensor_data
WHERE type = 'pressure'
GROUP BY site, HOUR(timestamp)
ORDER BY site, heure
```

---

## 📈 Intégration Power BI

### Export des données

```bash
python analysis/spark_analytics.py --analyse export
```

Génère: `reports/powerbi_export.csv`

### Configuration Power BI

1. **Importer les données**: Fichier → Obtenir des données → CSV
2. **Créer les visualisations**:
   - Graphique en courbes: Évolution temporelle
   - Histogramme: Alertes par type
   - Carte: Distribution par site
   - Tableau: Top 5 machines

### Exemple de dashboard

| Visualisation | Type | Données |
|--------------|------|---------|
| Température moyenne | Graphique en courbes | site, date, température |
| Alertes critiques | Histogramme | type, count alertes |
| Top 5 vibration | Tableau | machine, écart-type |
| Pression horaire | Graphique en aires | heure, pression moyenne |

---

## 🔧 Troubleshooting

### Erreur: "Java not found"
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$JAVA_HOME/bin:$PATH
```

### Erreur: "Delta Lake not found"
```bash
pip install delta-spark==3.0.0
```

### Problème de mémoire
```bash
export SPARK_DRIVER_MEMORY=4g
```

---

## 📝 Auteur

Projet réalisé dans le cadre du module **Data Warehouse et Big Data Warehouse**  
ENSA Khouribga - Filière Informatique et Ingénierie de Données

---

## 📜 Licence

Ce projet est à but éducatif uniquement.
