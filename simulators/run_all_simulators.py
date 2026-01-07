#!/usr/bin/env python3
"""
Script principal - Lance tous les simulateurs de capteurs
InduSense - Big Data Lakehouse Project
"""

import os
import sys
import threading
import time

# Ajouter le répertoire courant au path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from temperature_sensor import run_simulator as run_temperature
from vibration_sensor import run_simulator as run_vibration
from pressure_sensor import run_simulator as run_pressure

def run_all_simulators(num_readings=1000, parallel=True):
    """
    Lance tous les simulateurs de capteurs
    
    Args:
        num_readings: Nombre de mesures par capteur
        parallel: Si True, exécute les simulateurs en parallèle
    """
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Définir les répertoires de sortie
    temp_dir = os.path.join(script_dir, "../data_lake/raw/temperature/")
    vib_dir = os.path.join(script_dir, "../data_lake/raw/vibration/")
    press_dir = os.path.join(script_dir, "../data_lake/raw/pressure/")
    
    print("=" * 60)
    print("🏭 InduSense - Simulation de capteurs IoT industriels")
    print("=" * 60)
    print(f"   Mesures par capteur: {num_readings}")
    print(f"   Mode: {'Parallèle' if parallel else 'Séquentiel'}")
    print("=" * 60)
    print()
    
    start_time = time.time()
    
    if parallel:
        # Exécution parallèle avec threads
        threads = [
            threading.Thread(target=run_temperature, args=(num_readings, temp_dir)),
            threading.Thread(target=run_vibration, args=(num_readings, vib_dir)),
            threading.Thread(target=run_pressure, args=(num_readings, press_dir))
        ]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
    else:
        # Exécution séquentielle
        run_temperature(num_readings, temp_dir)
        print()
        run_vibration(num_readings, vib_dir)
        print()
        run_pressure(num_readings, press_dir)
    
    end_time = time.time()
    
    print()
    print("=" * 60)
    print(f"🎉 Simulation complète terminée!")
    print(f"   Total mesures générées: {num_readings * 3}")
    print(f"   Temps d'exécution: {end_time - start_time:.2f} secondes")
    print("=" * 60)

if __name__ == "__main__":
    # Par défaut: 1000 mesures par capteur, exécution parallèle
    num_readings = 1000
    
    if len(sys.argv) > 1:
        try:
            num_readings = int(sys.argv[1])
        except ValueError:
            print("Usage: python run_all_simulators.py [nombre_de_mesures]")
            sys.exit(1)
    
    run_all_simulators(num_readings=num_readings, parallel=False)
