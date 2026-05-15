"""
dashboard.py
------------
PAINEL DE MONITORAMENTO

Este script é um servidor web simples (Flask) que exibe o painel visual.
Ele consulta a API de gerenciamento do RabbitMQ a cada segundo e envia
os dados para o navegador usando SSE (Server-Sent Events).

O que é SSE?
  É uma tecnologia que mantém a conexão HTTP aberta e deixa o servidor
  "empurrar" atualizações para o navegador automaticamente, sem o navegador
  precisar ficar fazendo refresh ou pedindo dados (polling).

Como executar:
  python dashboard.py

Depois, abra no navegador:
  http://localhost:5000
"""

from flask import Flask, Response, render_template, request, jsonify
import requests
import json
import time
import threading
import pika
import producer

app = Flask(__name__)

# URL da API de gerenciamento interna do RabbitMQ (Management Plugin)
RABBITMQ_API_OVERVIEW = "http://localhost:15672/api/overview"
RABBITMQ_API_QUEUES = "http://localhost:15672/api/queues"
AUTH = ("admin", "admin123")


@app.route("/")
def index():
    """Serve a página principal do dashboard."""
    return render_template("index.html")


def buscar_metricas():
    """
    Consulta a API do RabbitMQ e retorna os dados das filas e taxas globais.
    """
    try:
        # Pega as filas
        res_queues = requests.get(RABBITMQ_API_QUEUES, auth=AUTH, timeout=2)
        res_queues.raise_for_status()
        filas = res_queues.json()

        # Pega as taxas (publish / deliver globais)
        res_overview = requests.get(RABBITMQ_API_OVERVIEW, auth=AUTH, timeout=2)
        res_overview.raise_for_status()
        overview = res_overview.json()
        stats = overview.get("message_stats", {})

        dados_filas = {}
        for fila in filas:
            nome = fila.get("name")
            stats = fila.get("message_stats", {})
            if not isinstance(stats, dict):
                stats = {}
            dados_filas[nome] = {
                "messages": fila.get("messages_ready", 0),
                "consumers": fila.get("consumers", 0),
                "acked": stats.get("ack", 0),
                "publish_rate": stats.get("publish_details", {}).get("rate", 0.0),
                "deliver_rate": stats.get("deliver_get_details", {}).get("rate", 0.0)
            }

        return {
            "status": "ok",
            "publish_rate": stats.get("publish_details", {}).get("rate", 0.0),
            "deliver_rate": stats.get("deliver_get_details", {}).get("rate", 0.0),
            "queues": dados_filas
        }
    except Exception as e:
        return {"error": str(e)}


@app.route("/stream")
def stream():
    """
    Endpoint SSE: mantém a conexão aberta e envia métricas a cada 1 segundo.
    O navegador se conecta uma vez e fica recebendo atualizações automaticamente.
    """
    def gerador_eventos():
        while True:
            metricas = buscar_metricas()
            # Formato SSE: cada evento começa com "data: " e termina com "\n\n"
            yield f"data: {json.dumps(metricas)}\n\n"
            time.sleep(1)

    return Response(gerador_eventos(), content_type="text/event-stream")

@app.route("/api/produce", methods=["POST"])
def api_produce():
    try:
        data = request.json
        count = int(data.get("count", 0))
        queue = data.get("queue", "all")
        
        if count <= 0 or count > 1000:
            return jsonify({"error": "Contagem inválida. Deve ser entre 1 e 1000."}), 400
            
        routing_key = None
        if queue == "orders.payment":
            routing_key = "order.payment.new"
        elif queue == "orders.stock":
            routing_key = "order.stock.reserve"
        elif queue == "orders.notification":
            routing_key = "order.notify.confirm"
            
        # Run in background to avoid blocking Flask
        threading.Thread(target=producer.executar, args=(count, count, routing_key)).start()
        
        return jsonify({"status": "success", "message": f"Produzindo {count} mensagens..."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/consume", methods=["POST"])
def api_consume():
    try:
        data = request.json
        count = int(data.get("count", 0))
        queue = data.get("queue")
        
        if not queue:
            return jsonify({"error": "Fila não especificada."}), 400
            
        if count <= 0:
            return jsonify({"error": "Contagem inválida. Deve ser maior que 0."}), 400
            
        # Consume in background
        def consume_task(q_name, max_msgs):
            try:
                credentials = pika.PlainCredentials(AUTH[0], AUTH[1])
                params = pika.ConnectionParameters(host="localhost", credentials=credentials)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                
                consumed = 0
                
                def callback(ch, method, properties, body):
                    nonlocal consumed
                    time.sleep(0.2) # Atraso para dar tempo de aparecer na tela
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    consumed += 1
                    if consumed >= max_msgs:
                        ch.stop_consuming()
                
                # Consumidor real para o RabbitMQ registrar e a estrelinha aparecer
                channel.basic_consume(queue=q_name, on_message_callback=callback)
                
                # Timeout de segurança: se não tiver mensagens suficientes, ele desliga o consumidor
                timeout = max(3.0, max_msgs * 0.3)
                connection.call_later(timeout, lambda: channel.stop_consuming())
                
                channel.start_consuming()
                connection.close()
                print(f"[MANUAL CONSUMER] Consumiu {consumed} de {q_name}")
            except Exception as ex:
                print(f"[MANUAL CONSUMER] Erro: {ex}")
            
        threading.Thread(target=consume_task, args=(queue, count)).start()
        
        return jsonify({"status": "success", "message": f"Consumindo de {queue}..."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("Dashboard iniciado! Acesse: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
