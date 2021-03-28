#
#   single_log_simulation.py
#   Emulate a single log producer
#   Input: a CSV file
#   Outpt: JSON log to Kafka topic
#

import pandas as pd
import os.path
from kafka import KafkaProducer
import time

INPUT_CSV = "/Users/matteo/Desktop/37.csv"
KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "siae-pm"
INTERVAL = 1  # Seconds

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], acks="all") #IMPORTANT: sets acks to "all" in order to avoid data missing in case of errors

df = pd.read_csv(INPUT_CSV)
total_log = len(df.index)
print("Number of Logs: %s" % total_log)
print("Starting Simulation")

counter = 1
for index, row in df.iterrows():
    s = row.to_json().encode("utf-8")
    producer.send(KAFKA_TOPIC, value=s)
    print("%d of %d (%d%%)" % (counter, total_log, counter/total_log*100), end='\r')
    counter += 1
    time.sleep(INTERVAL)
print()
print("Done!")
