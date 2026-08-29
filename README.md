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