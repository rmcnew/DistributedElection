# run on the local host
import logging
import time

from shared.run import Run


class RunLocal(Run):

    def __init__(self):
        super(RunLocal, self).__init__()


def main():
    logging.basicConfig(format='%(message)s',
                        level=logging.INFO, )

    run_local = RunLocal()
    coordinator_count = run_local.parse_command_line()
    command_line = run_local.build_command_line()

    # start coordinator processes
    for index in range(1, coordinator_count + 1):
        logging.info("Starting coordinator {} of {}".format(index, coordinator_count))
        run_local.run_in_own_process(command_line)
        time.sleep(1)

    logging.info("Distributed election and edit distance calculations are starting.  Press Ctrl-C to stop.")
    # listen for Ctrl-C
    # coordinator subprocesses also receive the signal and begin shutdown
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break

    logging.info("\nStopping the coordinator processes . . .")
    for process in run_local.processes:
        logging.debug("Joining coordinator {}".format(process.my_id))
        process.join()
    logging.info("All coordinator processes stopped.  Exiting . . .")


if __name__ == "__main__":
    main()
