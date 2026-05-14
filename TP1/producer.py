"""
producer.py
-----------
O PRODUTOR é o responsável por CRIAR e ENVIAR pedidos para o RabbitMQ.
Imagine ele como a "loja online": quando um cliente compra algo, o produtor
gera um pedido em formato JSON e coloca na fila certa.

Como funciona resumidamente:
  1. Conecta ao RabbitMQ
  2. Cria o "exchange" (central de distribuição) e as filas, se ainda não existirem
  3. Gera N pedidos aleatórios e os envia, um por um

Execute com:
  python producer.py --total 5000
"""

import pika
import uuid
import json
import random
import time
import argparse
from datetime import datetime, timezone

# ── Configurações de conexão ───────────────────────────────────────
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "admin"
RABBITMQ_PASS = "admin123"
EXCHANGE_NAME  = "orders.exchange"   # nome do "hub" central de mensagens

# Cada mensagem vai para um desses "destinos" (routing keys).
# O RabbitMQ lê a routing key e decide em qual fila colocar a mensagem.
ROUTING_KEYS = [
    "order.payment.new",    # → fila de pagamento
    "order.stock.reserve",  # → fila de estoque
    "order.notify.confirm", # → fila de notificação
]

# Produtos fictícios para simular pedidos realistas
PRODUCTS = ["notebook", "smartphone", "tablet", "monitor", "headset", "keyboard", "mouse"]


def conectar():
    """Abre e retorna uma conexão com o RabbitMQ."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(params)


def criar_infraestrutura(channel):
    """
    Declara o exchange, as filas e faz os bindings (ligações entre eles).
    Isso é feito apenas uma vez; se já existir, o RabbitMQ ignora silenciosamente.
    """
    # O exchange do tipo "topic" roteia mensagens baseado em padrões (ex: order.payment.*)
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type="topic",
        durable=True,   # durable=True significa que sobrevive a reinicializações
    )

    # Dead Letter Exchange (DLX): para onde vão as mensagens que falharam
    channel.exchange_declare(
        exchange="orders.dlx",
        exchange_type="fanout",
        durable=True,
    )

    # Dead Letter Queue (DLQ): a fila de mensagens "mortas" / com falha
    channel.queue_declare(queue="orders.dlq", durable=True)
    channel.queue_bind(exchange="orders.dlx", queue="orders.dlq")

    # Configurações extras para todas as filas principais:
    # - quorum: o dado é replicado nos 3 nós do cluster (alta disponibilidade)
    # - ttl: mensagem expira após 60 segundos se ninguém consumir
    # - dead-letter: mensagem com falha vai para a DLQ automaticamente
    queue_config = {
        "x-queue-type":           "quorum",
        "x-dead-letter-exchange": "orders.dlx",
    }

    # Lista de filas: (nome da fila, padrão de routing key que ela aceita)
    filas = [
        ("orders.payment",      "order.payment.*"),   # só pagamentos
        ("orders.stock",        "order.stock.*"),      # só estoque
        ("orders.notification", "order.notify.*"),     # só notificações
    ]

    for nome_fila, padrao in filas:
        channel.queue_declare(queue=nome_fila, durable=True, arguments=queue_config)
        channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=nome_fila,
            routing_key=padrao,
        )

    print("[SETUP] Exchange, filas e bindings prontos.")


def gerar_pedido(numero: int) -> dict:
    """Gera um pedido fictício com dados aleatórios."""
    return {
        "event_id":   str(uuid.uuid4()),
        "order_id":   f"ORD-{numero:06d}",
        "customer_id": f"CUST-{random.randint(1, 500):04d}",
        "product_id": random.choice(PRODUCTS),
        "quantity":   random.randint(1, 5),
        "amount":     round(random.uniform(19.99, 4999.99), 2),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def executar(total: int, intervalo_log: int = 1000, target_routing_key: str = None):
    """Conecta ao RabbitMQ e envia `total` pedidos."""
    print(f"[PRODUTOR] Conectando ao RabbitMQ em {RABBITMQ_HOST}...")
    connection = conectar()
    channel = connection.channel()

    # Ativa confirmação de entrega: o RabbitMQ confirma que recebeu cada mensagem
    channel.confirm_delivery()

    criar_infraestrutura(channel)

    print(f"[PRODUTOR] Enviando {total:,} pedidos...\n")

    inicio = time.time()
    erros = 0

    for i in range(1, total + 1):
        routing_key = target_routing_key if target_routing_key else random.choice(ROUTING_KEYS)
        pedido = gerar_pedido(i)
        corpo = json.dumps(pedido).encode("utf-8")

        try:
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=corpo,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 2 = persistente (sobrevive a reinicialização)
                    content_type="application/json",
                    message_id=pedido["event_id"],
                ),
                mandatory=True,
            )
        except Exception as e:
            erros += 1
            print(f"  [ERRO] Pedido {i}: {e}")
            continue

        # Exibe progresso a cada `intervalo_log` mensagens
        if i % intervalo_log == 0:
            decorrido = time.time() - inicio
            taxa = i / decorrido
            print(f"  Enviados: {i:>7,} | Tempo: {decorrido:>6.1f}s | Taxa: {taxa:>8.0f} msg/s")

    decorrido = time.time() - inicio
    taxa = total / decorrido

    print(f"\n{'='*50}")
    print(f"  TOTAL ENVIADO : {total:,} pedidos")
    print(f"  ERROS         : {erros}")
    print(f"  TEMPO TOTAL   : {decorrido:.2f}s")
    print(f"  TAXA MÉDIA    : {taxa:.0f} msg/s")
    print(f"{'='*50}")

    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produtor de pedidos – TP01")
    parser.add_argument("--total", type=int, default=10000,
                        help="Quantos pedidos enviar (padrão: 10000)")
    parser.add_argument("--report", type=int, default=1000,
                        help="A cada quantos pedidos imprimir progresso (padrão: 1000)")
    args = parser.parse_args()

    executar(total=args.total, intervalo_log=args.report)