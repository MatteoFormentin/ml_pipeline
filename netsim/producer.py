import pandas as pd
from datetime import datetime, timezone
import simplejson

CHUNKSIZE = 100


class Producer:
    def __init__(self, producer_id, csv_path, kafka_broker, topic):
        self.run_flag = True
        self.producer_id = producer_id
        self.csv_path = csv_path
        self.topic = topic
        self.counter = 0
        # Create an iterator over the CSV file, this will divide the file in multiple small chunks that fit in memory
        self.reader = iter(pd.read_csv(
            self.csv_path, chunksize=CHUNKSIZE, sep=";"))
        self.chunk = None  # Current chunk pointer
        self.chunk_counter = 0  # Index of current line in the current chunk of the file
        self.chunk_size = 0  # Size of current chunk

        # IMPORTANT: sets acks to "all" in order to avoid data missing in case of errors
        self.kafka_broker = kafka_broker

    def produceOneLog(self):
        # Decide if a new chunk must be loaded (if current line equal to chunk lenght)
        if self.chunk_counter >= self.chunk_size:
            try:
                # Get next chunk as an array of lines
                self.chunk = next(self.reader).to_dict("records")
                self.chunk_counter = 0
                self.chunk_size = len(self.chunk)
            except StopIteration:  # Exception throw if file has been completly readed
                self.run_flag = False
                return

        # Get current line from the chunk array
        row = self.chunk[self.chunk_counter]
        # Add timestamp of injection in unix ms - for pipeline testing
        ms_now = round(datetime.now(
            timezone.utc).timestamp() * 1e3)

        row["idlink"] = self.producer_id
        row["ingestion_ms"] = ms_now

        # Convert line into JSON
        s = simplejson.dumps(row, ignore_nan=True).encode("utf-8")
        # Publish log on Kafka
        self.kafka_broker.send(self.topic, value=s)

        self.chunk_counter += 1
        self.counter += 1

    def isRunning(self):
        return self.run_flag

    def getProducerId(self):
        return self.producer_id

    def getProcessedLogs(self):
        return self.counter
