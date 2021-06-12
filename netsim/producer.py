import pandas as pd
import time
from threading import Thread
from datetime import datetime, timezone


class Producer(Thread):
    def __init__(self, producer_id, csv_path, kafka_broker, topic, interval):
        Thread.__init__(self)
        self.run_flag = True
        self.producer_id = producer_id
        self.csv_path = csv_path
        self.topic = topic
        self.interval = interval
        self.counter = 0

        # IMPORTANT: sets acks to "all" in order to avoid data missing in case of errors
        self.kafka_broker = kafka_broker

    def run(self):
        #self.total_log = len(df.index)
        self.counter = 0
        with pd.read_csv(self.csv_path, chunksize=100) as reader:
            for chunk in reader:
                # Process current chunck of file
                for index, row in chunk.iterrows():
                    if not self.run_flag:
                        return

                    # Add timestamp of injection in unix ms
                    ms_now = round(datetime.now(
                        timezone.utc).timestamp() * 1e3)

                    row["ingestion_ms"] = ms_now
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

    def getProcessedLogs(self):
        return self.counter
