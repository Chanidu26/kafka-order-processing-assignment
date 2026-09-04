import os
import random

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from retry import (
    process_with_retry,
    TemporaryProcessingError,
    PermanentProcessingError
)

from dlq import DLQProducer


# ==================================================
# Configuration
# ==================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
)

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
)

TOPIC = "orders"

GROUP_ID = "order-consumer-group"


# ==================================================
# Load Avro schema
# ==================================================

schema_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order.avsc"
)

with open(schema_path, "r") as schema_file:

    schema_str = schema_file.read()


# ==================================================
# Schema Registry
# ==================================================

schema_registry_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})


# ==================================================
# Avro Deserializer
# ==================================================

def dict_to_order(data, ctx):

    return data


avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    dict_to_order
)


# ==================================================
# Kafka Consumer
# ==================================================

consumer_conf = {

    "bootstrap.servers":
        KAFKA_BOOTSTRAP_SERVERS,

    "group.id":
        GROUP_ID,

    "auto.offset.reset":
        "earliest",

    "enable.auto.commit":
        False,

    "key.deserializer":
        StringDeserializer("utf_8"),

    "value.deserializer":
        avro_deserializer
}


consumer = DeserializingConsumer(
    consumer_conf
)


consumer.subscribe([TOPIC])


# ==================================================
# DLQ Producer
# ==================================================

dlq_schema_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "dlq_order.avsc"
)


dlq_producer = DLQProducer(

    KAFKA_BOOTSTRAP_SERVERS,

    SCHEMA_REGISTRY_URL,

    dlq_schema_path
)


# ==================================================
# Running average
# ==================================================

total_price = 0.0
order_count = 0


# ==================================================
# Process order
# ==================================================

def process_order(order):

    global total_price
    global order_count


    order_id = order["orderId"]

    product = order["product"]

    price = order["price"]


    print(
        f"\nProcessing Order {order_id}"
    )

    print(
        f"Product: {product}"
    )

    print(
        f"Price: {price:.2f}"
    )


    # ----------------------------------------------
    # Permanent validation
    # ----------------------------------------------

    if price < 0:

        raise PermanentProcessingError(
            "Price cannot be negative"
        )


    # ----------------------------------------------
    # Simulate temporary failures
    # ----------------------------------------------

    # Orders ending with 5 have a temporary
    # failure on the first two attempts.
    if order_id.endswith("5"):

        attempts = temporary_attempts.get(
            order_id,
            0
        )

        if attempts < 2:

            temporary_attempts[order_id] = (
                attempts + 1
            )

            raise TemporaryProcessingError(
                "Simulated temporary failure"
            )


    # ----------------------------------------------
    # Simulate permanent failures
    # ----------------------------------------------

    # Orders ending with 9 permanently fail.
    if order_id.endswith("9"):

        raise PermanentProcessingError(
            "Simulated permanent processing failure"
        )


    # ----------------------------------------------
    # Successful processing
    # ----------------------------------------------

    total_price += price

    order_count += 1

    average = total_price / order_count


    print(
        "Order processed successfully."
    )

    print(
        f"Running Average Price: "
        f"{average:.2f}"
    )


# Dictionary used for temporary failure simulation
temporary_attempts = {}


# ==================================================
# Main consumer loop
# ==================================================

def main():

    print(
        "Starting Order Consumer..."
    )

    print(
        "Waiting for orders...\n"
    )


    try:

        while True:

            message = consumer.poll(
                timeout=1.0
            )


            if message is None:

                continue


            if message.error():

                print(
                    f"Kafka error: "
                    f"{message.error()}"
                )

                continue


            order = message.value()


            try:

                process_with_retry(

                    lambda:
                        process_order(order),

                    max_retries=3,

                    initial_delay=1

                )


                # ----------------------------------
                # Commit only after successful
                # processing
                # ----------------------------------

                consumer.commit(
                    message=message
                )


            except PermanentProcessingError as error:

                print(
                    f"\nPermanent failure "
                    f"for order "
                    f"{order['orderId']}: "
                    f"{error}"
                )


                # ----------------------------------
                # Send to DLQ
                # ----------------------------------

                dlq_producer.send(

                    order=order,

                    error=error,

                    retry_count=3

                )


                # ----------------------------------
                # Commit original message so it
                # doesn't get processed forever
                # ----------------------------------

                consumer.commit(
                    message=message
                )


            except TemporaryProcessingError as error:

                print(
                    f"\nOrder permanently failed "
                    f"after retries: "
                    f"{order['orderId']}"
                )


                dlq_producer.send(

                    order=order,

                    error=error,

                    retry_count=3

                )


                consumer.commit(
                    message=message
                )


    except KeyboardInterrupt:

        print(
            "\nStopping consumer..."
        )


    finally:

        consumer.close()


if __name__ == "__main__":

    main()