# TP01 – Sistema de Pedidos com RabbitMQ

## O Cenário

Imagine que você trabalha na equipe de tecnologia de um e-commerce. É véspera de Black Friday e a loja espera receber **milhares de pedidos por segundo**. O problema clássico de sistemas tradicionais é simples: se 5.000 clientes clicam em "Comprar" ao mesmo tempo, o servidor tenta gravar 5.000 pedidos no banco de dados simultaneamente — e trava.

A solução que este projeto demonstra é o uso de um **Message Broker** (no caso, o RabbitMQ). Em vez de processar tudo na hora, o sistema coloca cada pedido em uma **fila** e um conjunto de serviços independentes consome essa fila no seu próprio ritmo. A aplicação não trava, os dados não se perdem, e você pode escalar adicionando mais "trabalhadores" sem mudar uma linha do sistema principal.

---

## A Arquitetura do Sistema

```
                          ┌─────────────────────────────────┐
                          │        RabbitMQ Cluster         │
                          │  (3 nós replicados via Docker)  │
                          │                                 │
[producer.py]  ──────────►│  Exchange: orders.exchange      │
  (gera pedidos)          │                                 │
                          │  Fila: orders.payment  ─────────┼──► [consumer_payment.py]
                          │  Fila: orders.stock    ─────────┼──► [consumer_stock.py]
                          │  Fila: orders.notification ─────┼──► [consumer_notification.py]
                          │  Fila: orders.audit    ─────────┼──► [consumer_audit.py]
                          │                                 │
                          └─────────────────────────────────┘
                                        ▲
                          [dashboard.py] consulta API do RabbitMQ
                          e exibe tudo em http://localhost:5000
```

**Peças do sistema:**

| Arquivo | Papel no sistema |
|---|---|
| `docker-compose.yml` | Sobe o cluster localmente (modo desenvolvimento) |
| `docker-stack.yml` | Sobe o cluster via Docker Swarm (modo orquestrado) |
| `init_cluster.sh` | Une os 3 nós em cluster (para docker-compose) |
| `init_cluster_swarm.sh` | Une os 3 nós em cluster (para Docker Swarm) |
| `producer.py` | O "cliente": gera pedidos aleatórios e os envia ao RabbitMQ |
| `consumer_payment.py` | Serviço que processa pagamentos (valida e aprova) |
| `consumer_stock.py` | Serviço que reserva o produto no estoque |
| `consumer_notification.py` | Serviço que envia a confirmação ao cliente (email/SMS/push) |
| `consumer_audit.py` | Serviço que registra um log de **todos** os eventos do sistema |
| `dashboard.py` | Servidor web do painel de monitoramento em tempo real |
| `benchmark.py` | Ferramenta para medir o throughput com diferentes configurações |

---

## Pré-requisitos

- **Docker Desktop** instalado e rodando
- **Python 3.9+** instalado
- **Git Bash** ou **WSL** (para executar os scripts `.sh` no Windows)

---

## PASSO 1 — Subir a Infraestrutura

Abra um terminal na pasta `TP1` e execute:

```bash
docker compose up -d
```

**O que este comando faz:**
Ele lê o arquivo `docker-compose.yml` e sobe **3 contêineres** do RabbitMQ (`rabbit1`, `rabbit2`, `rabbit3`), cada um simulando um servidor independente. O `-d` significa "detached" — ou seja, roda em segundo plano sem travar o terminal.

Aguarde cerca de 30 segundos para todos os nós inicializarem completamente.

**Como verificar que funcionou:**
Abra o navegador em `http://localhost:15672` — você verá o painel oficial de gerenciamento do RabbitMQ. Faça login com:
- Usuário: `admin`
- Senha: `admin123`

---

## PASSO 2 — Unir os Nós em Cluster

Ainda no terminal (use Git Bash ou WSL no Windows):

```bash
bash init_cluster.sh
```

**O que este comando faz:**
Por padrão, os 3 contêineres RabbitMQ não se conhecem. Este script entra em cada um deles via `docker exec` e executa os comandos do RabbitMQ para formá-los em um único **cluster**. No final, configura as filas para usar **Quorum Queues** — o tipo de fila que replica os dados entre os nós.

**Por que isso importa:**
Com Quorum Queues, se um dos servidores cair, os outros dois já têm uma cópia dos dados. O sistema não perde nenhum pedido.

---

## PASSO 3 — Instalar as Dependências Python

Execute apenas na primeira vez:

```bash
pip3 install -r requirements.txt
```

**O que instala:**
- `pika` — biblioteca Python para falar com o RabbitMQ via protocolo AMQP
- `flask` — mini framework web para servir o dashboard
- `requests` — para consultar a API REST do RabbitMQ
- `matplotlib` — para gerar gráficos de benchmark

---

## PASSO 4 — Ligar o Dashboard

Abra um **novo terminal** (mantenha-o aberto durante toda a demonstração) e execute:

```bash
python3 dashboard.py
```

**O que este comando faz:**
Sobe um servidor web local na porta 5000. Ele consulta a API interna do RabbitMQ a cada segundo e usa **SSE (Server-Sent Events)** para empurrar os dados em tempo real para o navegador — sem precisar dar refresh na página.

Acesse no navegador: **http://localhost:5000**

Você verá o **Mapa Topológico Interativo** da sua arquitetura (um diagrama de rede visual). Por enquanto os números de mensagens (`Msgs: 0`) nas filas estarão zerados e não haverá nenhum "Consumer" conectado nas pontas, pois ainda não enviamos nenhum pedido.

---

## PASSO 5 — O Roteiro de Demonstração (Versão Clean)

Siga este roteiro passo a passo na sua apresentação:

### 🎬 Cena 0: A Prova do Cluster Distribuído
*Objetivo: Provar que não é um programa simples, mas uma infraestrutura de 3 servidores.*

1. No terminal, digite: `docker exec rabbit1 rabbitmqctl cluster_status`
2. Mostre a seção `Running Nodes` com os 3 servidores listados.
> *"Professor, antes de rodar o código, repare que temos 3 nós independentes rodando e se comunicando perfeitamente em Quórum."*

---

### 🎬 Cena 1: O Envio em Massa e a "Caixa Preta"
*Objetivo: Mostrar o produtor gerando carga e provar que o Docker salvou tudo fisicamente.*

1. **Terminal:** `python3 producer.py --total 5000`
2. **Dashboard Topológico:** Mostre as bolinhas preenchendo as 3 filas (Pagamento, Estoque, Notificação).
> *"5.000 pedidos chegaram e os consumers estão desligados. Onde estão os dados? Estão salvos nos discos rígidos do nosso cluster Docker."*
3. **A Prova Real:** 
   - Abra o painel nativo: **http://localhost:15672** (admin/admin123)
   - Vá na aba **Queues** > clique em **orders.payment**
   - Desça até **Get Messages** > mude Requeue para **Yes** > clique em **Get Message(s)**.
   - Mostre o JSON cru com os dados do cliente na tela!

---

### 🎬 Cena 2: O Processamento e a Escalabilidade
*Objetivo: Mostrar os workers esvaziando a fila e como escalar adicionando mais máquinas.*

1. **Terminal 1:** `python3 consumer_payment.py`
   - *No Dashboard:* Uma estrela amarela aparece. O número de mensagens da fila cai e o contador `Feito:` sobe rapidamente.
2. **Terminal 2 e 3:** Abra novos terminais e rode `python3 consumer_payment.py` neles também.
   - *No Dashboard:* A estrela muda para `consumer (3)`. A fila esvazia 3x mais rápido.
> *"Não precisamos de servidores mais potentes, apenas adicionamos mais workers na mesma fila. O RabbitMQ divide os pedidos automaticamente sem duplicidade."*

---

### 🎬 Cena 3: Os Microsserviços
*Objetivo: Mostrar arquitetura desacoplada.*

1. **Terminais Novos:** Rode `python3 consumer_stock.py` e `python3 consumer_notification.py`.
   - *No Dashboard:* Novas estrelas aparecem e começam a drenar as outras duas filas de forma totalmente paralela e independente.

---

### 🎬 Cena 4: Tolerância a Falhas (O Grand Finale)
*Objetivo: Matar um servidor ao vivo para provar a resiliência.*

1. Com todos os scripts rodando e o dashboard animado, vá no terminal e chute o balde:
   - `docker stop rabbit2`
2. **No Dashboard:** O sistema pisca por 1 segundo e volta ao normal trabalhando com 2 nós. Nenhum pedido é perdido.
> *"Derrubar um servidor em um banco tradicional seria catastrófico. Como exigimos Quorum Queues, o Algoritmo Raft manteve os dados seguros nos nós sobreviventes!"*

*(Para reviver o nó depois: `docker start rabbit2`)*

Se quiser medir o throughput sem fazer a demonstração manual, use:

```bash
# Testa com 1 consumer e 10.000 mensagens
python3 benchmark.py --msgs 10000 --consumers 1

# Testa com 4 consumers (rode depois do anterior)
python3 benchmark.py --msgs 10000 --consumers 4

# Gera um gráfico PNG comparando os resultados
python3 benchmark.py --plot-only
```

**O que acontece:**
O script limpa as filas, roda o producer e sobe N consumers automaticamente, mede quanto tempo levou para processar todas as mensagens e calcula o throughput (msg/s). O `--plot-only` lê os resultados salvos e gera um gráfico `benchmark_plot.png`.

---

## Entendendo o Dashboard (Mapa Topológico)

A nova interface gráfica desenha toda a sua arquitetura em tempo real. É interativa, você pode arrastar os ícones e dar zoom.

| Indicador Visual | O que significa |
|---|---|
| 🟢 **Status Conectado** | O dashboard está recebendo dados em tempo real da API do RabbitMQ |
| 🟡/🔴 **API Offline** | O servidor perdeu conexão (O RabbitMQ pode ter caído) |
| 🔵 **Círculo Verde (producer)** | O gerador das mensagens (no caso, seu `producer.py`) |
| 🔺 **Triângulo Laranja (exchange)** | O roteador do RabbitMQ (`orders.exchange`) que decide para qual fila a mensagem vai |
| 🟦 **Caixa Azul (queue)** | Suas 4 filas. O contador `Msgs:` mostra em tempo real quantas mensagens estão aguardando nela |
| ⭐ **Estrela Amarela (consumer)** | O seu "worker". Ela só aparece conectada a uma fila quando você liga o script correspondente (ex: `consumer_payment.py`). Mostra também quantas mensagens já processou (`Feito: X`) |
| 🔵🟡 **Bolinhas (Animações)** | Mostram o tráfego em tempo real: **Cyan/Laranja** (entrando nas filas), **Amarelo** (sendo processado pelo consumer). |
| **Setas Negras** | O caminho da mensagem. As setas entre o exchange e a fila indicam a `Routing Key` (ex: `payment`, `order.#`) |

---

## Encerrando Tudo

Para parar os consumers: `Ctrl+C` em cada terminal.

Para desligar o cluster Docker:
```bash
docker compose down
```

Para desligar **e apagar os dados** das filas (útil para começar do zero):
```bash
docker compose down -v
```

---

## Deploy com Docker Swarm (Orquestrador)

O **Docker Compose** é ótimo para desenvolvimento local, mas **não é um orquestrador**. O **Docker Swarm** é: ele gerencia os containers automaticamente, reinicia serviços que caem, e foi projetado para rodar em múltiplas máquinas.

O projeto tem um arquivo próprio para isso: `docker-stack.yml`.

### O que muda na prática?

| | Docker Compose | Docker Swarm |
|---|---|---|
| Onde roda | Só na sua máquina | Uma ou várias máquinas |
| Rede | Bridge (local) | Overlay (entre máquinas) |
| Restart automático | Não | Sim |
| Comando para subir | `docker compose up` | `docker stack deploy` |
| Ver serviços | `docker ps` | `docker service ls` |

### Passo a Passo com Swarm

**1. Ativar o modo Swarm na sua máquina:**
```bash
docker swarm init
```
> Isso transforma sua máquina em um **manager node** do Swarm. Em uma infraestrutura real, você adicionaria outras máquinas como **worker nodes** com o token gerado por este comando.

**2. Fazer o deploy do cluster RabbitMQ via Swarm:**
```bash
docker stack deploy -c docker-stack.yml tp01
```
> O Swarm lê o `docker-stack.yml` e cria os 3 serviços RabbitMQ. Aguarde ~30 segundos.

**3. Verificar se os serviços subiram:**
```bash
docker service ls
```
Você deve ver `tp01_rabbit1`, `tp01_rabbit2` e `tp01_rabbit3` com `1/1` réplicas.

**4. Unir os nós em cluster (use Git Bash ou WSL):**
```bash
bash init_cluster_swarm.sh
```
> Este script é a versão do `init_cluster.sh` adaptada para o Swarm. A diferença é que no Swarm os containers não têm nomes fixos — o script os localiza automaticamente pelo nome do serviço.

**5. Acessar o painel web:**
Abra `http://localhost:15672` — deve mostrar os 3 nós no cluster.

**6. O restante do roteiro é igual** — use o mesmo `producer.py`, consumers e `dashboard.py`.

### Demonstrando a Tolerância a Falhas do Swarm

Derrube um serviço:
```bash
docker service scale tp01_rabbit2=0
```

Aguarde e observe o Swarm **reiniciando automaticamente**:
```bash
docker service scale tp01_rabbit2=1
```

> **O que mostrar na apresentação:** diferente do `docker stop` que apenas mata o container, com o Swarm demonstramos que o *orquestrador detecta a falha e age sozinho*, sem intervenção humana. Isso é o que separa orquestração de simples containerização.

### Removendo tudo do Swarm

```bash
# Remove a stack inteira
docker stack rm tp01

# Sai do modo Swarm (volta ao modo normal)
docker swarm leave --force
```

---

## Deploy na Nuvem AWS (EC2)

Para tornar o painel acessível para qualquer pessoa (ex: pelo celular), podemos subir toda a infraestrutura para a AWS usando o **EC2** e o **Docker**.

### Passo a Passo Básico:

1. **Crie uma Instância EC2:**
   - Acesse o Console da AWS.
   - Inicie uma instância EC2 usando a imagem **Ubuntu Server**.
   - Tipo de instância: `t2.micro` (gratuita) ou `t3.small` (se precisar de mais recursos para o RabbitMQ).

2. **Libere as Portas no Security Group:**
   Na configuração de rede da EC2, permita tráfego de entrada para as seguintes portas:
   - `22` (SSH - para você acessar o terminal)
   - `5000` (Para o Dashboard Interativo Flask)
   - `15672` (Para o Painel de Controle do RabbitMQ)

3. **Acesse a Máquina via SSH e Instale o Docker:**
   ```bash
   # Atualize a máquina
   sudo apt update && sudo apt upgrade -y
   
   # Instale o Docker e o Docker Compose
   sudo apt install docker.io docker-compose -y
   
   # Instale o Python e pip
   sudo apt install python3 python3-pip -y
   ```

4. **Transfira o Projeto:**
   Você pode clonar este repositório via `git clone <seu-repo>` diretamente dentro da máquina EC2.

5. **Inicie a Infraestrutura na Nuvem:**
   Dentro da pasta do projeto na EC2:
   ```bash
   # Sobe o cluster RabbitMQ
   sudo docker-compose up -d
   
   # Une os nós do RabbitMQ
   sudo bash init_cluster.sh
   
   # Instala dependências e liga o Dashboard
   pip3 install -r requirements.txt
   python3 dashboard.py
   ```

6. **Acesse pelo Celular:**
   Abra o navegador e digite o **IP Público da sua EC2** seguido da porta:
   `http://<IP-PUBLICO-DA-EC2>:5000`
   
   O painel topológico vai carregar e você e seus amigos poderão usar a nova interface para **Produzir** e **Consumir** mensagens interativamente de onde estiverem!
