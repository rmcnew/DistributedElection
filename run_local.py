# run on the local host
import logging
import time

from shared.run import Run


class RunLocal(Run):

    def __init__(self):
        super(RunLocal, self).__init__()


def main():
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d : %(message)s',
                        level=logging.INFO)

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

    while not run_local.processes_done():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break

    if not run_local.processes_done():
        # shutting down due to Ctrl-C keyboard interrupt
        logging.info("\nSending shutdown directive to coordinator processes . . .")
        run_local.send_shutdown_message()
        time.sleep(2)
        logging.info("Waiting for coordinator processes to shutdown . . .")
        for process in run_local.processes:
            process.join()
    logging.info("All coordinator processes stopped.  Exiting . . .")


if __name__ == "__main__":
    main()
