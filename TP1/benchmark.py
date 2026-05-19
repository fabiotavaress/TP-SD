import argparse
import subprocess
import time
import requests
import json
import os
import matplotlib.pyplot as plt

RESULTS_FILE = "benchmark_results.json"
RABBIT_API = "http://localhost:15672/api/queues/%2f/orders.payment"
AUTH = ("admin", "admin123")

def get_queue_messages():
    try:
        resp = requests.get(RABBIT_API, auth=AUTH)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("messages", 0)
    except Exception as e:
        print(f"[API_ERRO] {e}")
    return -1

def purge_queues():
    print(" Limpando filas antes do teste...")
    queues = ["orders.payment", "orders.stock", "orders.notification", "orders.audit", "orders.dlq"]
    for q in queues:
        url = f"http://localhost:15672/api/queues/%2f/{q}/contents"
        try:
            requests.delete(url, auth=AUTH)
        except:
            pass
    time.sleep(2)

def run_benchmark(msgs, consumers_count):
    purge_queues()
    
    print(f"\n[BENCHMARK] Gerando {msgs} mensagens...")
    subprocess.run(["python", "producer.py", "--total", str(msgs), "--target", "order.payment.new"], check=True)
    
    print(f"\n[BENCHMARK] Iniciando {consumers_count} consumidores...")
    processes = []
    # Usaremos o consumer_payment como base para testar throughput real
    for _ in range(consumers_count):
        p = subprocess.Popen(["python", "consumer_payment.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(p)
    
    start_time = time.time()
    
    # Aguarda a fila esvaziar
    while True:
        m_count = get_queue_messages()
        if m_count == 0:
            # Garanta que nao ha mensagens unacked na API pra ter ctz
            break
        # timeout preventivo (30 mins max)
        if time.time() - start_time > 1800:
            print("[BENCHMARK] Timeout excedido!")
            break
        time.sleep(1)
        
    elapsed = time.time() - start_time
    rate = msgs / elapsed if elapsed > 0 else 0
    
    print(f"\n[RESULTADO] {consumers_count} consumidores processaram {msgs} msgs em {elapsed:.2f}s => {rate:.0f} msg/s")
    
    # Finalizando processos
    for p in processes:
        p.terminate()
        
    save_result(consumers_count, rate)
    generate_plot()

def save_result(consumers, rate):
    results = {}
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r") as f:
            try:
                results = json.load(f)
            except:
                pass
    results[str(consumers)] = rate
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f)

def generate_plot():
    if not os.path.exists(RESULTS_FILE):
        return
        
    with open(RESULTS_FILE, "r") as f:
        results = json.load(f)
        
    if not results: return
    
    x = sorted([int(k) for k in results.keys()])
    y = [results[str(k)] for k in x]
    
    plt.figure(figsize=(8, 5))
    plt.plot(x, y, marker='o', linestyle='-', color='b', linewidth=2)
    plt.title("Throughput do RabbitMQ por Número de Consumidores")
    plt.xlabel("Número de Consumidores (orders.payment)")
    plt.ylabel("Mensagens / Segundo (Vazão)")
    plt.xticks(x)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("benchmark_plot.png")
    print("\n[PLOT] Grafico gerado e salvo como 'benchmark_plot.png'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Benchmark do RabbitMQ")
    parser.add_argument("--msgs", type=int, help="Total de msgs para simular")
    parser.add_argument("--consumers", type=int, help="Numero de consumidores")
    parser.add_argument("--plot-only", action="store_true", help="Gera apenas o grafico com base nos resultados ja gravados")
    args = parser.parse_args()
    
    if args.plot_only:
        generate_plot()
    elif args.msgs and args.consumers:
        run_benchmark(args.msgs, args.consumers)
    else:
        print("Uso: python benchmark.py --msgs X --consumers Y")
