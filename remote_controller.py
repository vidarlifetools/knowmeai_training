"""Simple example of remote controller for the system"""
import argparse
from pick import pick
import time
from subprocess import Popen

import zmq
from framework.message import CONTROL_PORT
import framework.message as message

EXIT = "EXIT"
RESTART = "RESTART"

MESSAGES = {
    "SHUTDOWN": message.SHUTDOWN,
    RESTART: None,
    EXIT: None,
}


def main(args):
    options = list(MESSAGES.keys())
    sock = zmq.Context().socket(zmq.PUB)
    sock.connect(f"tcp://{args.remote_host}:{CONTROL_PORT}")

    option = None

    while option != EXIT:
        option, _ = pick(options, "System controller")
        sock.send_pyobj(MESSAGES[option])

        if option == RESTART:
            time.sleep(1)
            Popen(["python", "main.py", "--config", "config.json"])

    sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--remote-host", help="Sensor PC IP or name", default="localhost"
    )

    main(parser.parse_args())
