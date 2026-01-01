# Temperature sensor data generator
import random
import time
from datetime import datetime


def generate_temperature_reading():
    """Generate a random temperature reading."""
    return {
        "sensor_type": "temperature",
        "value": round(random.uniform(15.0, 35.0), 2),
        "unit": "celsius",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    while True:
        reading = generate_temperature_reading()
        print(reading)
        time.sleep(1)
