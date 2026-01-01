# Pressure sensor data generator
import random
import time
from datetime import datetime


def generate_pressure_reading():
    """Generate a random pressure reading."""
    return {
        "sensor_type": "pressure",
        "value": round(random.uniform(900.0, 1100.0), 2),
        "unit": "hPa",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    while True:
        reading = generate_pressure_reading()
        print(reading)
        time.sleep(1)
