# Kafka Order Processing System

A Kafka-based order processing system demonstrating:

- Apache Kafka
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
Kafka
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

Starts Kafka, Schema Registry, and Kafka UI.

```bash
docker compose up -d
```

Verify the containers are healthy:

```bash
docker compose ps
curl http://localhost:8081/subjects        # Schema Registry -> 200 OK
```

Kafka UI is available at http://localhost:8080

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
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic orders
```

Check that the consumer group is caught up (`LAG` should be `0`):

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-consumer-group
```

Or inspect messages, topics, and consumer group lag visually in Kafka UI at http://localhost:8080

### 6. Tear down

Stop the producer/consumer with `CTRL+C`, then stop the infrastructure:

```bash
docker compose down -v
```

`-v` also removes the Kafka data volume, so the topic history is wiped on next startup.