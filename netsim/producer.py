import pandas as pd
from datetime import datetime, timezone
import simplejson

CHUNKSIZE = 20


class Producer:
    def __init__(self, producer_id, csv_path, kafka_broker, topic):
        self.run_flag = True
        self.producer_id = producer_id
        self.csv_path = csv_path
        self.topic = topic
        self.counter = 0
        self.reader = iter(pd.read_csv(self.csv_path, chunksize=CHUNKSIZE, sep=";"))
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
                return

        row = self.chunk[self.chunk_counter]
        # Add timestamp of injection in unix ms
        ms_now = round(datetime.now(
            timezone.utc).timestamp() * 1e3)

        row["idlink"] = self.producer_id
        row["ingestion_ms"] = ms_now

        s = simplejson.dumps(row, ignore_nan=True).encode("utf-8")
        self.kafka_broker.send(self.topic, value=s)

        self.chunk_counter += 1
        self.counter += 1

    def isRunning(self):
        return self.run_flag

    def getProducerId(self):
        return self.producer_id

    def getProcessedLogs(self):
        return self.counter
