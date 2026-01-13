
import json
import threading
import uuid
import random
import time
import os
from datetime import datetime

# Configuration
OUTPUT_DIR = "../data_lake/raw/pressure/"
SITES = ["Site_Paris", "Site_Lyon", "Site_Marseille", "Site_Toulouse", "Site_Nantes"]
MACHINES = ["Machine_A1", "Machine_A2", "Machine_B1", "Machine_B2", "Machine_C1"]

# Plages de pression (en Pascal - Pa, ou bar pour lisibilité)
# Valeurs en bar (1 bar = 100000 Pa)
PRESSURE_NORMAL_MIN = 1.0
PRESSURE_NORMAL_MAX = 5.0
PRESSURE_CRITICAL_MIN = 6.0
PRESSURE_CRITICAL_MAX = 10.0

def generate_pressure_reading():
    """Génère une mesure de pression"""
    
    # 8% de chance d'avoir une valeur critique (alerte)
    if random.random() < 0.08:
        value = round(random.uniform(PRESSURE_CRITICAL_MIN, PRESSURE_CRITICAL_MAX), 2)
    else:
        value = round(random.uniform(PRESSURE_NORMAL_MIN, PRESSURE_NORMAL_MAX), 2)
    
    reading = {
        "sensor_id": str(uuid.uuid4()),
        "type": "pressure",
        "value": value,
        "unit": "bar",
        "site": random.choice(SITES),
        "machine": random.choice(MACHINES),
        "timestamp": datetime.now().isoformat()
    }
    
    return reading

def save_reading(reading, output_dir):
    """Sauvegarde la mesure dans un fichier JSON"""
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    # Nom de fichier unique basé sur timestamp et UUID
    filename = f"press_{reading['timestamp'].replace(':', '-')}_{reading['sensor_id'][:8]}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(reading, f, ensure_ascii=False, indent=2)
    
    return filepath

def run_simulator(num_readings=1000, output_dir=OUTPUT_DIR):
    """
    Lance le simulateur de pression
    
    Args:
        num_readings: Nombre de mesures à générer (minimum 1000)
        output_dir: Répertoire de sortie
    """
    print(f" Démarrage du simulateur de pression")
    print(f"   Répertoire de sortie: {output_dir}")
    print(f"   Nombre de mesures à générer: {num_readings}")
    print("-" * 50)
    
    for i in range(num_readings):
        # Générer et sauvegarder la mesure
        reading = generate_pressure_reading()
        filepath = save_reading(reading, output_dir)
        
        # Afficher le progrès
        if (i + 1) % 100 == 0:
            print(f"    {i + 1}/{num_readings} mesures générées")
        
        # Délai aléatoire entre 1 et 3 secondes
        # time.sleep(random.uniform(1, 3))
    
    print("-" * 50)
    print(f"Simulation terminée: {num_readings} mesures de pression générées")

def run_realtime_simulator(output_dir, interval_sec=2):
    stop_event = threading.Event()

    """
    Simulateur temps réel (1 événement = 1 fichier)
    """
    print(" Démarrage simulateur pression (REAL-TIME)")
    print(f"   Répertoire: {output_dir}")
    print(f"   Intervalle: {interval_sec}s")
    print("-" * 50)
    try:
        while not (stop_event and stop_event.is_set()):
            reading = generate_pressure_reading()
            filepath = save_reading(reading, output_dir)

            print(
                f"  {reading['timestamp']} | "
                f"{reading['site']} | "
                f"{reading['machine']} | "
                f"{reading['value']} bar"
            )

            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\nArrêt du simulateur pression (REAL-TIME) demandé par l'utilisateur.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Simulateur pression")
    parser.add_argument(
        "--mode",
        choices=["batch", "realtime"],
        default="batch"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=2,
        help="Intervalle en secondes (mode realtime)"
    )

    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, OUTPUT_DIR)

    if args.mode == "batch":
        run_simulator(num_readings=1000, output_dir=output_dir)
    else:
        run_realtime_simulator(
            output_dir=output_dir,
            interval_sec=args.interval
        )
