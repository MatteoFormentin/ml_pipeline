import argparse
import os
import time
import kafka
import sys
from kafka import KafkaProducer
import datetime


from csv_splitter import split_csv
from producer import Producer


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
            self.finished_log_processed = 0
            self.batch_counter = 1
            starting_time = datetime.datetime.now()
            if args.limit:
                self.limit = args.limit
            else:
                self.limit = -1

            try:
                # Create Kafka connection
                print("Connecting to Kafka...")
                self.kafka_broker = KafkaProducer(
                    bootstrap_servers=[self.kafka_broker], acks="all")
                print("Connected to Kafka")
            except Exception:
                print("No kafka brokers available, maybe wrong IP address?. Retry.")
                sys.exit(-1)

            # Create log producers classes
            self.runProducers()
            print("Created %d producers                                                                  " % len(
                self.producers))

            try:
                # Check if there are still log producer acrive
                while not len(self.producers) == 0:
                    batch_log_processed = 0
                    prod_counter = 0
                    #Send one log to the pipeline one log foreach producer
                    for p in self.producers:
                        if p.isRunning():
                            p.produceOneLog()
                            batch_log_processed += p.getProcessedLogs()

                        else:
                            print("Producer %d finish simulation. %d logs injected.                             " %
                                  (p.getProducerId(), p.getProcessedLogs()))
                            self.finished_log_processed += p.getProcessedLogs()

                            # If the producer has no more log in the csv, stop and remove it
                            self.producers.remove(p)

                        prod_counter += 1

                        # Print an awesome progress bar on the terminal
                        self.printProgressBar(
                            prod_counter, len(self.producers), "Publishing batch %d " % (self.batch_counter), length=50)

                    print("Batch %d done. Active producers: %d. Total injected logs: %d. Pausing until next batch.                                                          " %
                          (self.batch_counter, len(self.producers), batch_log_processed), end='\r')
                    self.batch_counter += 1
                    time.sleep(self.interval)

            #Stop if ctrl+c
            except KeyboardInterrupt:
                for p in self.producers:
                    if p.isRunning():
                        self.finished_log_processed += p.getProcessedLogs()

            self.printStats(starting_time, self.finished_log_processed)

        # Run CSV splitter
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
        # Parse command line argument

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
        #Load csv file from the provided path and create one producer class foreach file
        spawned_count = 1
        folder = os.listdir(self.input_folder)
        folder.sort()

        try:
            for file_name in folder:
                full_path = os.path.join(self.input_folder, file_name)
                if os.path.isfile(full_path) and file_name.endswith(".csv") and not file_name.startswith("."):
                    producer_id = int(file_name.split(".")[0])
                    p = Producer(producer_id, full_path,
                                 self.kafka_broker, self.kafka_topic)
                    self.producers.append(p)
                    spawned_count += 1
                    if spawned_count > self.limit:
                        break

                self.printProgressBar(
                    spawned_count, self.limit, "Creating producers ", length=50)

        except KeyboardInterrupt:
            print()
            print("Aborted. Exiting.")
            sys.exit()

    def areJobsTerminated(self):
        for p in self.producers:
            if p.isRunning():
                return False
        return True

    def printStats(self, starting_time, log_processed):
        # Calculate time elapsed
        s = (datetime.datetime.now() - starting_time).seconds
        hours = s // 3600
        s = s - (hours * 3600)
        minutes = s // 60
        seconds = s - (minutes * 60)
        print("Simulation done. Injected %s Logs. Took %d hours %d minutes %d seconds                                     " % (
            log_processed, hours, minutes, seconds))

    def printProgressBar(self, iteration, total, prefix='', suffix='', decimals=1, length=100, printEnd="\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                         (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = "=" * (filledLength - 1) + '>' + ' ' * (length - filledLength)
        print(f'\r{prefix} [{bar}] {percent}% {suffix}', end=printEnd)
        # Print New Line on Complete
        # if iteration == total:
        #    print()
