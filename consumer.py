from aws_glue_schema_registry import AvroDeserializer
from aws_glue_schema_registry.config import GlueSchemaRegistryConfiguration
from confluent_kafka import Consumer

# Kafka config
consumer_config = {
    "bootstrap.servers": "<your-msk-bootstrap>:9092",
    "group.id": "glue-avro-group",
    "auto.offset.reset": "earliest"
}

# Glue Registry config
glue_config = GlueSchemaRegistryConfiguration(region_name="us-east-1")
deserializer = AvroDeserializer(glue_config)

# Kafka consumer
consumer = Consumer(consumer_config)
consumer.subscribe(["user-avro-topic"])

print("⏳ Listening for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("⚠️", msg.error())
            continue

        decoded_data = deserializer.deserialize(msg.value())
        print("✅ Received:", decoded_data)
except KeyboardInterrupt:
    print("🛑 Shutting down.")
finally:
    consumer.close()
