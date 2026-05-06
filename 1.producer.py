import time
import random
import json
from kafka import KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW

TIME_GAP_MESSAGE = 2

def main():
    producer = KafkaProducer(
        bootstrap_servers=[f"{BROKER_ADDR}:{BROKER_PORT}"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"[*] Sensor iniciado. Enviando dados para {TOPIC_RAW} em {BROKER_ADDR}:{BROKER_PORT}")

    try:
        while True:
            # Simula leitura de temperatura
            temperatura = round(random.uniform(20.00, 30.00), 2)
            data = {
                'sensor_id': 'sensor-01', # Estatico
                'value': temperatura,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            print(f"[+] Enviando: {data}")
            producer.send(TOPIC_RAW, value=data)
            producer.flush()
            
            time.sleep(TIME_GAP_MESSAGE)
    except KeyboardInterrupt:
        print("[*] Sensor parado.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
