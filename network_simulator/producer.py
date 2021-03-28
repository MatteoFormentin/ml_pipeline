import pandas as pd
import os.path
from kafka import KafkaProducer
import time
from threading import Thread


class Producer(Thread):
    def __init__(self, producer_id, csv_path, kafka_address, topic, interval):
        Thread.__init__(self)
        self.run_flag = True
        self.producer_id = producer_id
        self.csv_path = csv_path
        self.topic = topic
        self.interval = interval
        self.counter = 0
        self.total_log = 0

        # IMPORTANT: sets acks to "all" in order to avoid data missing in case of errors
        self.kafka_broker = KafkaProducer(
            bootstrap_servers=[kafka_address], acks="all")

    def run(self):
        df = pd.read_csv(self.csv_path)
        self.total_log = len(df.index)

        self.counter = 1
        for index, row in df.iterrows():
            if not self.run_flag:
                return

            s = row.to_json().encode("utf-8")
            self.kafka_broker.send(self.topic, value=s)

            self.counter += 1
            time.sleep(self.interval)

        self.run_flag = False

    def stop(self):
        self.run_flag = False

    def isRunning(self):
        return self.run_flag

    def getProducerId(self):
        return self.producer_id

    def getTotalLogs(self):
        return self.total_log

    def getProcessedLogs(self):
        return self.counter
