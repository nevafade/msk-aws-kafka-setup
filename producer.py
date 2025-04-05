from aws_glue_schema_registry import AvroSerializer
from aws_glue_schema_registry.config import GlueSchemaRegistryConfiguration
from confluent_kafka import Producer
import json

# Kafka config (MSK bootstrap server)
producer_config = {
    "bootstrap.servers": "<your-msk-bootstrap>:9092"
}

# AWS Glue Registry config
glue_config = GlueSchemaRegistryConfiguration(
    region_name="us-east-1",  # your AWS region
    registry_name="my-registry"
)

# Avro schema
schema_definition = """
{
  "type": "record",
  "name": "User",
  "namespace": "example.avro",
  "fields": [
    { "name": "name", "type": "string" },
    { "name": "age", "type": "int" },
    { "name": "email", "type": ["null", "string"], "default": null }
  ]
}
"""

# Initialize serializer
serializer = AvroSerializer(glue_config)

# Initialize Kafka producer
producer = Producer(producer_config)

# Data to send
record = {
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
}

# Serialize the message
avro_bytes = serializer.serialize(
    data=record,
    schema=schema_definition,
    schema_name="User"
)

# Send to Kafka
producer.produce(topic="user-avro-topic", value=avro_bytes)
producer.flush()
print("✅ Message sent using AWS Glue Schema Registry!")
