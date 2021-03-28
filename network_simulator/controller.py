import argparse
import os
from csv_splitter import split_csv
from producer import Producer
import time

REFRESH_RATE = 1


class Controller():
    def __init__(self):
        pass

    def run(self):
        args = self.parseArgs()
        input_folder = args.input
        self.kafka_broker = args.kafka
        self.kafka_topic = args.topic
        self.interval = 15 * 60

        self.producers = []

        if args.interval:
            self.interval = args.interval

        # Check if required splitting
        if args.split:
            if args.out:  # Check if out folder
                out_folder = args.out
            else:
                out_folder = "./"
            print("Splitting the file. It may require some time.")
            split_csv(self.input, args.split, out_folder)
            print("Split done.")
            input_folder = out_folder
        elif args.out:
            raise argparse.ArgumentTypeError(
                "Out flag set without splitting required")

        print("Spawning producers...")
        self.runProducers(input_folder)
        print("Active producers: %d" % len(self.producers))
        print()

        last_measure = time.time()
        while not self.areJobsTerminated():
            if time.time() - last_measure > REFRESH_RATE:
                for p in self.producers:
                    if p.isRunning():
                        print("Producer %s progress: %d of %d (%d%%)" %
                              (p.getProducerId(), p.getProcessedLogs(), p.getTotalLogs(),
                               p.getProcessedLogs() / p.getTotalLogs() * 100))
                    else:
                        print("Producer %s finished simulation." %
                              (p.getProducerId()))
                print()
                last_measure = time.time()

        print("Simulation done.")

    def parseArgs(self):
        parser = argparse.ArgumentParser(
            description="Utility to simulate many logs producer that inject logs into Kafka instance")
        parser.add_argument(
            "input", type=str,  help="Set input files directory or input file if splitting required.")

        parser.add_argument(
            "kafka", type=str, help="Kafka instance to produce log. Must be in form host:port.")

        parser.add_argument(
            "topic", type=str, help="Set Kafka topic where to publish logs.")

        parser.add_argument(
            "--split", "-s", type=str, help="Split a single CSV file into one per producer using the passed value for splitting.")

        parser.add_argument(
            "--out", "-o", type=str, help="Set where to store splitted file. Only required if split should be performed")

        parser.add_argument(
            "--interval", "-i", type=float, help="Set time interval between each log publish in seconds. Default 15 minutes")

        return parser.parse_args()

    def runProducers(self, files_path):
        for path in os.listdir(files_path):
            full_path = os.path.join(files_path, path)
            if os.path.isfile(full_path) and full_path.endswith(".csv"):
                producer_id = path.split(".")[0]
                p = Producer(producer_id, full_path, self.kafka_broker,
                             self.kafka_topic, self.interval)
                p.start()
                self.producers.append(p)

    def areJobsTerminated(self):
        for p in self.producers:
            if p.isRunning():
                return False
        return True
