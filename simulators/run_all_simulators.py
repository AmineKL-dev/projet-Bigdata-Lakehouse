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

from temperature_sensor import (
    run_simulator as run_temperature,
    run_realtime_simulator as run_temperature_rt
)

from vibration_sensor import (
    run_simulator as run_vibration,
    run_realtime_simulator as run_vibration_rt
)

from pressure_sensor import (
    run_simulator as run_pressure,
    run_realtime_simulator as run_pressure_rt
)


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
    print(" InduSense - Simulation de capteurs IoT industriels")
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
    print(f" Simulation complète terminée!")
    print(f"   Total mesures générées: {num_readings * 3}")
    print(f"   Temps d'exécution: {end_time - start_time:.2f} secondes")
    print("=" * 60)

def run_all_simulators_realtime(interval=2, parallel=True):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "../data_lake/raw/temperature/")
    vib_dir = os.path.join(script_dir, "../data_lake/raw/vibration/")
    press_dir = os.path.join(script_dir, "../data_lake/raw/pressure/")

    print("📡 REAL-TIME (Ctrl+C pour arrêter)")
    if parallel:
        threads = [
            threading.Thread(target=run_temperature_rt, args=(temp_dir, interval), daemon=True),
            threading.Thread(target=run_vibration_rt, args=(vib_dir, interval), daemon=True),
            threading.Thread(target=run_pressure_rt, args=(press_dir, interval), daemon=True),
        ]
        for t in threads:
            t.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Arrêt demandé")
    else:
        run_temperature_rt(temp_dir, interval)
        run_vibration_rt(vib_dir, interval)
        run_pressure_rt(press_dir, interval)



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="InduSense - Lancement des simulateurs IoT"
    )

    parser.add_argument(
        "--mode",
        choices=["batch", "realtime"],
        default="batch"
    )

    parser.add_argument(
        "--num_readings",
        type=int,
        default=1000,
        help="Nombre de mesures par capteur (mode batch)"
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=2,
        help="Intervalle en secondes (mode realtime)"
    )

    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Exécution parallèle"
    )

    args = parser.parse_args()

    if args.mode == "batch":
        run_all_simulators(
            num_readings=args.num_readings,
            parallel=args.parallel
        )
    else:
        run_all_simulators_realtime(
            interval=args.interval,
            parallel=True
        )

