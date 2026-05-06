import json
import threading
import grpc
from concurrent import futures
from kafka import KafkaConsumer
from const import BROKER_ADDR, BROKER_PORT, TOPIC_PROCESSED, GRPC_PORT
import SensorService_pb2
import SensorService_pb2_grpc

db_readings = [] # o banco de dados
db_lock = threading.Lock()

class SensorServiceServicer(SensorService_pb2_grpc.SensorServiceServicer):
    # Ler a ultima
    def GetLatestReading(self, request, context):
        with db_lock:
            if not db_readings:
                return SensorService_pb2.ReadingResponse(value=0.0, timestamp="N/A")
            latest = db_readings[-1]
            return SensorService_pb2.ReadingResponse(
                value=latest['avg_value'],
                timestamp=latest['timestamp']
            )

    # Ler todas
    def GetHistory(self, request, context):
        with db_lock:
            readings = [
                SensorService_pb2.ReadingResponse(value=r['avg_value'], timestamp=r['timestamp'])
                for r in db_readings
            ]
            return SensorService_pb2.HistoryResponse(readings=readings)

def kafka_consumer_thread():
    print(f"[*] Kafka Consumer iniciado. Escutando {TOPIC_PROCESSED}...")
    try:
        consumer = KafkaConsumer( # Consumir o que o 2.processor.py enviar
            TOPIC_PROCESSED,
            bootstrap_servers=[f"{BROKER_ADDR}:{BROKER_PORT}"],
            auto_offset_reset='earliest',
            group_id='web-service-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            data = message.value
            print(f"[\033[94mKAFKA\033[0m] Recebido: {data}")
            with db_lock:
                db_readings.append(data) # Salvar no 'bd'
                # Poderia limitar o bd, improvavel de ser necessario
                """ if len(db_readings) > 1000:
                    db_readings.pop(0) """
    except Exception as e:
        print(f"[\033[91mERRO KAFKA\033[0m] {e}")

def serve():
    # Inicia o consumidor Kafka em uma thread separada
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()

    # Inicia o servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    SensorService_pb2_grpc.add_SensorServiceServicer_to_server(SensorServiceServicer(), server)
    server.add_insecure_port(f'[::]:{GRPC_PORT}')
    print(f"[*] Web Service gRPC iniciado na porta {GRPC_PORT}...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
