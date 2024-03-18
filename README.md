## Kafka Utility Modules

This repository contains utility modules for interacting with Apache Kafka, including an AdminClient for administrative operations, a Producer for producing messages, and a Consumer for consuming messages.

## Import All Modules
```python
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import KafkaException
from utility.kafka import consumer, producer, admin_client
```

## Admin Client Module

## Description:
This module provides an AdminClient class for interacting with Apache Kafka administrative operations such as creating topics, checking topic existence, and deleting topics.

## Requirements
- Python 3.x installed.
- `kafka-python` library installed (`pip install kafka-python`).
- `confluent_kafka` library installed (`pip install confluent_kafka`).

## Usage:

### Initialize AdminClient
```python
configuration = {
	"bootstrap_servers":"localhost:9092",
	"client_id":'test',
}
admin = admin_client.AdminClient(configuration)
```

### Create a topic
```python
topic_config = {
    "name": "test-topic",
    "num_partitions": 3,
    "replication_factor": 1
}
admin.create_topic(topic_config):
```

### Check if a topic exists
```python
admin.topic_exists("test-topic")
```

### Delete a topic
```python
admin.delete_topic("test-topic")
```

### Close the connection
```python
admin.close()
```

## Producer Module

## Description:
This module provides a Producer class for producing messages to Kafka topics.

## Usage:

### Initialize Producer
```python
p_config = {
    "bootstrap_servers": "localhost:9092",
    "value_serializer": lambda v: v.encode('utf-8'),
    "key_serializer": lambda v: str(v).encode(),
    "acks": 0
}
kafka_producer = producer.Producer(p_config)
```

### Produce a message
```python
kafka_producer.produce("test-topic","Hello, kafka!")
```
### Close the connection
```python
kafka_producer.close()
```

## Consumer Module

## Description:
This module provides a Consumer class for consuming messages from Kafka topics.

## Usage:

### Initialize Consumer
```python
c_config = {
    "bootstrap_servers": "kafka:9092",
    "enable_auto_commit": True,
    "auto_offset_reset": "latest",
    "request_timeout_ms": 3 * 1000
}
kafka_consumer = consumer.Consumer(c_config)
```

### Consume latest message
```python
kafka_consumer.consume_latest("test-topic")
```

### Close the connection
```python
kafka_consumer.close()
```
