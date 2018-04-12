# parent class for all runner classes
import argparse
import logging
import os
import signal
import subprocess
import sys
from multiprocessing import Process

import boto3

from messages import *


class Run:

    def __init__(self):
        self.sns = boto3.resource(SNS, region_name='us-west-2')
        self.topic = self.sns.create_topic(Name=LFDE_SNS_TOPIC)
        self.processes = []

    def parse_command_line(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(COUNT, type=int, help="number of Coordinator processes to run")
        args = parser.parse_args()
        if args.count:
            count = args.count
            if count < 2:
                print("At least two Coordinators are needed to do work (one Overseer and one Worker)")
                exit(-1)
            elif count > MAX_COORDINATOR_PROCESSES:
                print("A maximum of {} Coordinators are allowed".format(MAX_COORDINATOR_PROCESSES))
                exit(-2)
            else:
                return count
        else:
            print("Usage: {} [COUNT]"
                  "\nwhere COUNT is the desired number of Coordinator processes to run".format(sys.argv[0]))
            exit(0)

    def send_shutdown_message(self):
        self.topic.publish(Message=shutdown_message())

    def get_python_interpreter(self):
        return sys.executable

    def get_script_folder(self):
        return os.path.dirname(os.path.realpath(sys.argv[0]))

    def build_command_line(self):
        return "{} {}".format(self.get_python_interpreter(),
                              os.path.join(self.get_script_folder(), COORDINATOR_SCRIPT_NAME))

    def run_as_subprocess(self, command_line):
        split_command_line = command_line.split(' ')
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        completed = subprocess.run(split_command_line, stdin=None)
        return completed

    def run_in_own_process(self, command_line):
        logging.debug("Launching process with command_line: {}".format(command_line))
        process = Process(target=self.run_as_subprocess, args=(command_line,))
        self.processes.append(process)
        process.start()

    def processes_done(self):
        for process in self.processes:
            if process.is_alive():
                return False
        return True
