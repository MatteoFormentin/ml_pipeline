import pandas as pd
from threading import Thread
from datetime import datetime, timezone
import json

CHUNKSIZE = 10


class Producer:
    def __init__(self, producer_id, csv_path, kafka_broker, topic, interval):
        self.run_flag = True
        self.producer_id = producer_id
        self.csv_path = csv_path
        self.topic = topic
        self.interval = interval
        self.counter = 0
        self.reader = iter(pd.read_csv(self.csv_path, chunksize=CHUNKSIZE))
        self.chunk = None
        self.chunk_counter = 0
        self.chunk_size = 0

        # IMPORTANT: sets acks to "all" in order to avoid data missing in case of errors
        self.kafka_broker = kafka_broker

    def produceOneLog(self):

        if self.chunk_counter >= self.chunk_size:
            try:
                self.chunk = next(self.reader).to_dict("records")
                self.chunk_counter = 0
                self.chunk_size = len(self.chunk)
            except StopIteration:
                self.run_flag = False

        else:
            row = self.chunk[self.chunk_counter]
            # Add timestamp of injection in unix ms
            ms_now = round(datetime.now(
                timezone.utc).timestamp() * 1e3)

            row["ingestion_ms"] = ms_now
            s = json.dumps(row).encode("utf-8")
            self.kafka_broker.send(self.topic, value=s)

            self.chunk_counter += 1
            self.counter += 1

    def isRunning(self):
        return self.run_flag

    def getProducerId(self):
        return self.producer_id

    def getProcessedLogs(self):
        return self.counter
