# Kafka Order Processing System

A Kafka-based order processing system demonstrating:

- Apache Kafka (3-broker, fault-tolerant cluster)
- Avro serialization
- Confluent Schema Registry
- Real-time running average
- Retry logic
- Dead Letter Queue (DLQ)

## Architecture

Producer
    |
    | Avro
    v
Kafka (3-broker cluster, replication factor 3)
    |
    | Avro
    v
Consumer
    |
    +---- Successful processing
    |          |
    |          v
    |    Running Average
    |
    +---- Temporary Failure
    |          |
    |          v
    |        Retry
    |
    +---- Permanent Failure
               |
               v
          orders.DLQ

## Technologies

- Python
- Apache Kafka
- Confluent Schema Registry
- Apache Avro
- Docker
- Docker Compose

## Project Structure

```text
kafka-order-processing/
│
├── docker-compose.yml
│
├── schemas/
│   ├── order.avsc
│   └── dlq_order.avsc
│
├── producer/
│   ├── producer.py
│   └── requirements.txt
│
├── consumer/
│   ├── consumer.py
│   ├── retry.py
│   ├── dlq.py
│   └── requirements.txt
│
├── README.md
│
└── .gitignore
```

## Setup & Running

### Prerequisites

- Docker Desktop (with Docker Compose)
- Python 3.11+

### 1. Start the Kafka infrastructure

Starts a 3-broker Kafka cluster (`kafka1`, `kafka2`, `kafka3` — KRaft mode, no ZooKeeper), Schema Registry, and Kafka UI.

```bash
docker compose up -d
```

Verify the containers are healthy:

```bash
docker compose ps
curl http://localhost:8081/subjects        # Schema Registry -> 200 OK
```

Kafka UI is available at http://localhost:8080

#### Fault tolerance

Internal topics and auto-created topics (`orders`, `orders.DLQ`) use replication factor 3 with `min.insync.replicas=2`, so the cluster tolerates 1 broker failure with zero downtime and no data loss. You can verify this yourself:

```bash
# check current leader/replicas/ISR for the orders topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders

# kill the current leader broker (e.g. kafka3) and watch it fail over
docker stop kafka3
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092,kafka2:29092 --describe --topic orders
# -> a new leader is elected automatically, producer/consumer keep working

# bring it back — it rejoins and re-syncs
docker start kafka3
```

Each broker is reachable from the host on a different port: `kafka1` → `localhost:9092`, `kafka2` → `localhost:9093`, `kafka3` → `localhost:9094`. The producer/consumer default `KAFKA_BOOTSTRAP_SERVERS` lists all three so they can still connect even if the first broker in the list is down.

### 2. Create a virtual environment and install dependencies

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r producer/requirements.txt
pip install -r consumer/requirements.txt
```

### 3. Run the producer

Sends a new random order to the `orders` topic every second.

```bash
python producer/producer.py
```

### 4. Run the consumer

In a separate terminal (with the venv activated), start the consumer to process orders, compute the running average, retry temporary failures, and route permanent failures to `orders.DLQ`.

```bash
python consumer/consumer.py
```

### 5. Verify things are working

Check that the producer is writing messages:

```bash
docker exec kafka1 kafka-get-offsets --bootstrap-server kafka1:29092 --topic orders
```

Check that the consumer group is caught up (`LAG` should be `0`):

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group order-consumer-group
```

Or inspect messages, topics, and consumer group lag visually in Kafka UI at http://localhost:8080

### 6. Tear down

Stop the producer/consumer with `CTRL+C`, then stop the infrastructure:

```bash
docker compose down -v
```

`-v` also removes the Kafka data volume, so the topic history is wiped on next startup.