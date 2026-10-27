import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor_data'

print("Iniciando envío de datos de sensores a Kafka...")

try:
    while True:
        data = {
            'sensor_id': random.randint(1, 10),
            'temperature': round(random.uniform(20.0, 30.0), 2),
            'humidity': round(random.uniform(30.0, 70.0), 2),
            'timestamp': int(time.time())
        }
        producer.send(topic_name, value=data)
        print(f"Sent: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Deteniendo productor...")
finally:
    producer.close()
