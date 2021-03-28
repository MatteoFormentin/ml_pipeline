#
#   kafka_consumer.py
#   Consume Kafka topic. For test porpuse
#
KAFKA_BROKER = "localhost:9093"

from kafka import KafkaConsumer
# auto_offset_reset sets wich message of the queue use as starting point NB: if setted to "latest" all the message before start of consumer will be ignored,
# However if setted to "earliest" at each start all the old message will be processed possibly lead to duplicates
consumer = KafkaConsumer(
    'siae-pm', bootstrap_servers=KAFKA_BROKER, auto_offset_reset="latest")
count = 0
for msg in consumer:
    #print(msg)
    count +=1
    print(count, end='\r')
