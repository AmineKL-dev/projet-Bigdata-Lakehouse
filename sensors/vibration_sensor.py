# Vibration sensor data generator
import random
import time
from datetime import datetime


def generate_vibration_reading():
    """Generate a random vibration reading."""
    return {
        "sensor_type": "vibration",
        "value": round(random.uniform(0.0, 10.0), 3),
        "unit": "mm/s",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    while True:
        reading = generate_vibration_reading()
        print(reading)
        time.sleep(1)
