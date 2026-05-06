import json
from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_RAW, TOPIC_PROCESSED, WINDOW_SIZE

def main():
    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=[f"{BROKER_ADDR}:{BROKER_PORT}"], # Lendo o que o 1.producer.py produzir
        auto_offset_reset='earliest',
        group_id='processor-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = KafkaProducer( # Enviar a nova mensagem
        bootstrap_servers=[f"{BROKER_ADDR}:{BROKER_PORT}"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    readings = []

    print(f"[*] Processador iniciado. {TOPIC_RAW} -> {TOPIC_PROCESSED}")

    for message in consumer:
        data = message.value
        readings.append(data['value'])
        
        if len(readings) > WINDOW_SIZE:
            readings.pop(0) # Retirar o elemento mais antigo
            
        media_recente_temperatura = round(sum(readings) / len(readings), 2)
        
        processed_data = {
            'avg_value': media_recente_temperatura,
            'last_value': data['value'],
            'timestamp': data['timestamp'],
            'count': len(readings)
        }
        
        print(f"[#] Processado: Média={media_recente_temperatura} (último: {data['value']})")
        producer.send(TOPIC_PROCESSED, value=processed_data)
        producer.flush()

if __name__ == "__main__":
    main()
