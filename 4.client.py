import grpc
import time
from const import GRPC_ADDR, GRPC_PORT, WINDOW_SIZE
import SensorService_pb2
import SensorService_pb2_grpc

def run():
    with grpc.insecure_channel(f'{GRPC_ADDR}:{GRPC_PORT}') as channel:
        stub = SensorService_pb2_grpc.SensorServiceStub(channel)
        
        print("[*] Cliente gRPC conectado.")
        
        while True:
            print("\n\033[95m======= MENU DO CLIENTE =======\033[0m")
            print(f"1. \033[92m[QUERY]\033[0m Ver média das últimas {WINDOW_SIZE} temperaturas")
            print("2. \033[92m[QUERY]\033[0m Ver histórico")
            print("3. \033[91m[SAIR]\033[0m Encerrar")
            print("\033[95m===============================\033[0m")
            
            choice = input("Escolha uma opção: ")
            
            if choice == '1':
                try:
                    response = stub.GetLatestReading(SensorService_pb2.Empty())
                    print(f"\n\033[93m>> RESULTADO:\033[0m Média de \033[1m{response.value}°C\033[0m em {response.timestamp}")
                except Exception as e:
                    print(f"\033[91mErro:\033[0m {e}")
                    
            elif choice == '2':
                try:
                    response = stub.GetHistory(SensorService_pb2.Empty())
                    print("\n\033[93m>> HISTÓRICO DE MÉDIAS:\033[0m")
                    if not response.readings:
                        print("   (Nenhum dado disponível no momento)")
                    else:
                        for r in response.readings:
                            print(f"   [\033[90m{r.timestamp}\033[0m] \033[1m{r.value}°C\033[0m")
                except Exception as e:
                    print(f"\033[91mErro:\033[0m {e}")
                    
            elif choice == '3':
                break
            else:
                print("Opção inválida.")

if __name__ == "__main__":
    run()
