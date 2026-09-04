import random
import time
import os

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
)

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
)

TOPIC = "orders"


# --------------------------------------------------
# Load Avro schema
# --------------------------------------------------

schema_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order.avsc"
)

with open(schema_path, "r") as schema_file:
    schema_str = schema_file.read()


# --------------------------------------------------
# Schema Registry
# --------------------------------------------------

schema_registry_conf = {
    "url": SCHEMA_REGISTRY_URL
}

schema_registry_client = SchemaRegistryClient(
    schema_registry_conf
)


# --------------------------------------------------
# Avro serialization function
# --------------------------------------------------

def order_to_dict(order, ctx):
    return order


avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    order_to_dict
)


# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    "key.serializer": StringSerializer("utf_8"),

    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)


# --------------------------------------------------
# Generate random order
# --------------------------------------------------

products = [
    "Item1",
    "Item2",
    "Item3",
    "Item4",
    "Item5"
]


def generate_order(order_id):

    return {
        "orderId": str(order_id),
        "product": random.choice(products),
        "price": round(random.uniform(100, 1000), 2)
    }


# --------------------------------------------------
# Delivery callback
# --------------------------------------------------

def delivery_report(err, msg):

    if err is not None:

        print(
            f"Delivery failed: {err}"
        )

    else:

        print(
            f"Produced Order | "
            f"ID: {msg.key()} | "
            f"Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    order_id = 1000

    print("Starting Order Producer...")
    print("Press CTRL+C to stop.\n")

    try:

        while True:

            order_id += 1

            order = generate_order(order_id)

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report
            )

            print(
                f"Order created: "
                f"{order['orderId']} | "
                f"{order['product']} | "
                f"Price: {order['price']:.2f}"
            )

            producer.poll(0)

            time.sleep(1)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()


if __name__ == "__main__":
    main()