#
#   kafka_consumer.py
#   Consume Kafka topic. For test porpuse
#
KAFKA_BROKER = "localhost:9093"

from kafka import KafkaConsumer
consumer = KafkaConsumer('test', bootstrap_servers=KAFKA_BROKER)
for msg in consumer:
    print(msg)
