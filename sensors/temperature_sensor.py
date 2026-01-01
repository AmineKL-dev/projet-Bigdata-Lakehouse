import json, uuid, random, time
from datetime import datetime

while True:
    data = {
        "sensor_id": str(uuid.uuid4()),
        "type": "temperature",
        "value": round(random.uniform(10, 90), 2),
        "unit": "Celsius",
        "site": random.choice(["Site_A", "Site_B"]),
        "machine": random.choice(["Machine_1", "Machine_2", "Machine_3"]),
        "timestamp": datetime.now().isoformat()
    }

    filename = f"data_lake/raw/temperature/{uuid.uuid4()}.json"
    with open(filename, "w") as f:
        json.dump(data, f)

    time.sleep(random.randint(1, 3))
