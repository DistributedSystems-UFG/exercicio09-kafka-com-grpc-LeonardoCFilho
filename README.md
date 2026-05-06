[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/A6uVSc3Y)

# Exercício 09

## Setup do Kafka

Roda em terminal próprio.

```bash
sudo apt update
sudo apt install default-jdk
wget https://dlcdn.apache.org/kafka/4.2.0/kafka_2.13-4.2.0.tgz
tar -xzf kafka_2.13-4.2.0.tgz
cd kafka_2.13-4.2.0
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.
bin/kafka-server-start.sh config/server.properties
```

## Execução do código

### Instalação das dependências

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### Compilação do gRPC

```bash
python3 -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/SensorService.proto
```

### Terminal 2

```bash
python3 3.consumer.py
```

### Terminal 3

```bash
python3 2.processor.py
```

### Terminal 4

```bash
python3 1.producer.py
```

### Terminal 5

```bash
python3 4.client.py
```
