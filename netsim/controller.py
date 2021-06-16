import argparse
import os
import time
import kafka
import sys
from kafka import KafkaProducer
import datetime


from csv_splitter import split_csv
from producer import Producer

REFRESH_RATE = 1


class Controller():
    def __init__(self):
        pass

    def run(self):
        print
        print("    )             (                ")
        print(" ( /(           ) )\ )             ")
        print(" )\())   (   ( /((()/( (      )    ")
        print("((_)\   ))\  )\())/(_)))\    (     ")
        print(" _((_) /((_)(_))/(_)) ((_)   )\    ")
        print("| \| |(_))  | |_ / __| (_) _((_))  ")
        print("| .` |/ -_) |  _|\__ \ | || '  \() ")
        print("|_|\_|\___|  \__||___/ |_||_|_|_|  ")
        print("                                   ")
        print()

        args = self.parseArgs()

        if args.command == "simulate":
            print("Simulation Module v1.0")
            print()
            self.producers = []
            self.kafka_broker = args.kafka_broker
            self.kafka_topic = args.kafka_topic
            self.input_folder = args.input_folder
            self.interval = args.interval
            starting_time = datetime.datetime.now()
            if args.limit:
                self.limit = args.limit
            else:
                self.limit = -1

            try:
                print("Connecting to Kafka...")
                self.kafka_broker = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker], acks="all")
                print("Connected to Kafka")
            except kafka.errors:
                print("No kafka brokers available. Retry.")
                sys.exit(-1)

            print("Spawning producers...")
            self.runProducers()
            print("Spawned producers: %d" % len(self.producers))


            
            while not len(self.producers) == 0:
                total_log_processed = 0
                for p in self.producers:
                    total_log_processed += p.getProcessedLogs()
                    if p.isRunning():
                        p.produceOneLog()

                    else:
                        print("Producer %s finish simulation. %d logs injected.                             " %
                              (p.getProducerId(), p.getProcessedLogs()))
                        self.producers.remove(p)
                        
                print("Active producers: %d. Injected Logs: %d" %
                      (len(self.producers), total_log_processed), end='\r')
                
                time.sleep(self.interval)


            # Calculate time elapsed
            s = (datetime.datetime.now() - starting_time).seconds
            hours = s // 3600
            s = s - (hours * 3600)
            minutes = s // 60
            seconds = s - (minutes * 60)
            print("Simulation done. Injected %s Logs. Took %d hours %d minutes %d seconds                                     " % (
                total_log_processed, hours, minutes, seconds))

        # Check if required splitting
        elif args.command == "split":
            print("Split Module v1.0")
            print()

            print("Splitting the file. It may require some time.")
            split_csv(args.input_csv, args.column_name, args.out_folder)
            print("Split done.")

        else:
            print("No command provided.")
            print()
            self.parser.print_help()

    def parseArgs(self):
        self.parser = argparse.ArgumentParser(
            description="Utilities to handle and simulate logs producer")

        function_choose_parser = self.parser.add_subparsers(dest="command")

        # Parser for simulate command
        sim_parser = function_choose_parser.add_parser(
            "simulate", help="Utility to simulate many logs producer that inject logs into Kafka instance")
        sim_parser.add_argument(
            "input_folder", type=str,  help="Set logs files directory.")

        sim_parser.add_argument(
            "kafka_broker", type=str, help="Kafka instance to produce log. Must be in form host:port.")

        sim_parser.add_argument(
            "kafka_topic", type=str, help="Set Kafka topic where to publish logs.")

        sim_parser.add_argument(
            "--interval", "-i", type=float, default=15*60, help="Set time interval between each log publish in seconds. Default 15 minutes")

        sim_parser.add_argument(
            "--limit", "-l", type=int, default=100, help="Set how many producer run. Additional CSV log will be ignored.")

        # Parser for split command
        split_parser = function_choose_parser.add_parser(
            "split", help="Utility to prepare CSV logs for simulation. Split a single CSV file that contains many producers log combined into one CSV per producer.")
        split_parser.add_argument(
            "input_csv", type=str,  help="Set input file to perform splitting on.")

        split_parser.add_argument(
            "out_folder", type=str, help="Set where to store splitted file.")

        split_parser.add_argument(
            "column_name", type=str, help="Set the name of the column used to perform the split. Should be a value that uniquely identify each log producer")

        return self.parser.parse_args()

    def runProducers(self):
        spawned_count = 0
        for path in os.listdir(self.input_folder):
            full_path = os.path.join(self.input_folder, path)
            if os.path.isfile(full_path) and full_path.endswith(".csv"):
                producer_id = path.split(".")[0]

                p = Producer(producer_id, full_path, self.kafka_broker,
                             self.kafka_topic, self.interval)
                self.producers.append(p)
                spawned_count += 1
                if spawned_count >= self.limit:
                    break

    def areJobsTerminated(self):
        for p in self.producers:
            if p.isRunning():
                return False
        return True
