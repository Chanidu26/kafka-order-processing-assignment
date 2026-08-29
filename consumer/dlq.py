from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


DLQ_TOPIC = "orders.DLQ"


def dlq_to_dict(message, ctx):
    return message


class DLQProducer:

    def __init__(
        self,
        bootstrap_servers,
        schema_registry_url,
        schema_path
    ):

        # ------------------------------------------
        # Load DLQ Avro schema
        # ------------------------------------------

        with open(schema_path, "r") as file:

            schema_str = file.read()


        # ------------------------------------------
        # Schema Registry
        # ------------------------------------------

        schema_registry_client = SchemaRegistryClient({
            "url": schema_registry_url
        })


        # ------------------------------------------
        # Avro Serializer
        # ------------------------------------------

        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            dlq_to_dict
        )


        # ------------------------------------------
        # Producer
        # ------------------------------------------

        self.producer = SerializingProducer({

            "bootstrap.servers": bootstrap_servers,

            "key.serializer": StringSerializer("utf_8"),

            "value.serializer": avro_serializer

        })


    def send(
        self,
        order,
        error,
        retry_count
    ):

        dlq_message = {

            "orderId": order["orderId"],

            "product": order["product"],

            "price": order["price"],

            "error": str(error),

            "retryCount": retry_count

        }


        self.producer.produce(

            topic=DLQ_TOPIC,

            key=order["orderId"],

            value=dlq_message

        )


        self.producer.flush()


        print(
            f"\nMessage sent to DLQ: "
            f"{DLQ_TOPIC}"
        )