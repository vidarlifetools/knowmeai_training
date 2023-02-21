import argparse
import json
import multiprocessing
from dataclasses import dataclass
import tracemalloc
import os
import logging
import networkx
import zmq
import time
import socket
from multiprocessing import Process, set_start_method
from multiprocessing import Event


from framework.module import Module
import framework.message as message

# Include module references
from processes.update_files import update_files, MODULE_UPDATE_FILES
from processes.update_features import update_features, MODULE_UPDATE_FEATURES
from processes.label import label, MODULE_LABEL
from processes.train import train, MODULE_TRAIN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:Controller:%(message)s",
)

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('192.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

TRACE_MEM = False
TRACE_INTERVAL = 3600

MODULE_CONTROLLER = "Controller"

MODULES = {
    MODULE_UPDATE_FILES: update_files,
    MODULE_UPDATE_FEATURES: update_features,
    MODULE_LABEL: label,
    MODULE_TRAIN: train
}
HOST_IPS = {
    MODULE_UPDATE_FILES: "192.168.10.215",
    MODULE_UPDATE_FEATURES: "192.168.10.215",
    MODULE_LABEL: "192.168.10.215",
    MODULE_TRAIN: "192.168.10.215"
}

@dataclass
class ControllerConfig:
    module_connections: None
    module_configs: dict

class Controller(Module):
    """Controller for all data modules"""

    def __init__(self, cfg):
        self.status_sock = zmq.Context().socket(zmq.SUB)
        self.status_sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.status_sock.bind(f"tcp://*:{message.STATUS_PORT}")

        # self.control_sock = zmq.Context().socket(zmq.PAIR)
        self.control_sock = zmq.Context().socket(zmq.SUB)
        self.control_sock.setsockopt_string(zmq.SUBSCRIBE, "")
        self.control_sock.bind(f"tcp://*:{message.CONTROL_PORT}")

        self.stop_event = multiprocessing.Event()
        self.start_event = multiprocessing.Event()

        self.config = cfg
        self.setup_modules(cfg.module_connections)

    def setup_modules(self, modules):
        assert modules
        self.modules = dict()
        local_ip = get_local_ip()
        #logging.info(f"This machine has IP address {local_ip}")
        data_ports = dict(zip(modules.nodes, range(60060, 60080)))

        for n in modules.nodes:
            ins = [f"tcp://{HOST_IPS[x]}:{data_ports[x]}" for x in modules.predecessors(n)]

            out = None
            if list(modules.successors(n)):
                #out = f"tcp://*:{data_ports[n]}"
                out = f"tcp://*:{data_ports[n]}"
                #logging.info(f"Module {str(n)} has port {data_ports[n]} and IP address {HOST_IPS[n]}")

            config = self.config.module_configs.get(n)
            # Only include modules that runs on the same machine, have same IP address
            if local_ip == HOST_IPS[n]:
                #logging.info(f"{n} is running on this machine")

                self.modules[n] = Process(target=MODULES[n], args=(
                    self.start_event, self.stop_event, config, f"tcp://localhost:{message.STATUS_PORT}", ins, out ))
#                self.modules[n] = MODULES[n](
#                    config, f"tcp://localhost:{message.STATUS_PORT}", ins, out
#                )
            #else:
                #logging.info(f"{n} is running on a machine with IP address {HOST_IPS[n]}")

        logging.info("Initialized")

    def start(self):
        for m in self.modules.values():
            m.start()
            logging.info(f"Started module: {m}")
        self.start_event.set()

        super().start()
        logging.info(f"Started. Remote control port: {message.CONTROL_PORT}")

    def stop(self):

        self.stop_event.set()

        #for m in self.modules.values():
        #    m.stop()

        logging.info(f"Stopped all modules: {[m.name for m in self.modules.values()]}")
        self.is_running = False
        self.status_sock.close()
        self.control_sock.close()
        logging.info("Stopped")

    def loop(self):
        last_stat_time = time.time()
        while self.is_running:
            try:
                status_msg = self.status_sock.recv_multipart(zmq.NOBLOCK)[1]
            except:
                status_msg = None

            try:
                control_msg = self.control_sock.recv_pyobj(zmq.NOBLOCK)
                # self.control_sock.send_pyobj("OK")
            except:
                # print(e)
                control_msg = None

            if status_msg is not None:
                print(status_msg.decode("utf-8"))

            self.process_control_msg(control_msg)
            time.sleep(0.1)

            if TRACE_MEM and time.time() - last_stat_time > TRACE_INTERVAL:
                print("Memory usage:")
                print(snap())
                last_stat_time = time.time()

    def process_control_msg(self, msg):
        if not msg:
            return

        if isinstance(msg, str):
            if msg == message.SHUTDOWN:
                self.stop()

        elif isinstance(msg, tuple) and msg[0] == message.CONFIG:
            self.configure(msg[1], msg[2], msg[3])
        else:
            logging.info(f"Unknown message: {msg}")

    def configure(self, client_name, grab_duration, post_duration):
        logging.info(f"Changing client name to {client_name}")
        #self.modules[MODULE_GRABBER].set_client(client_name)
        logging.info(f"Changing grab duration to ({grab_duration}, {post_duration})")
        #self.modules[MODULE_GRABBER].set_grab_duration(grab_duration, post_duration)
        args = parser.parse_args()
        config = json.load(open(args.config))
        config["Grabber"]["client_name"] = client_name
        config["Grabber"]["buffer_size"] = grab_duration
        config["Grabber"]["post_buffer_size"] = post_duration
        json.dump(config, open(args.config, "w"), indent=4)


def parse_config(json_dict: dict) -> ControllerConfig:
    module_connections = networkx.DiGraph()
    for src, dists in json_dict[MODULE_CONTROLLER]["connections"].items():
        for d in dists:
            module_connections.add_edge(src, d)

    # Remove the Controller part og the config dict
    json_dict.pop(MODULE_CONTROLLER)
    return ControllerConfig(module_connections, json_dict)


s = None
def snap():
    global s
    if not s:
        s = tracemalloc.take_snapshot()
        return("Memory allocation snapshot taken")
    else:
        lines = []
        top_stats = tracemalloc.take_snapshot().compare_to(s, "lineno")
        for stat in top_stats:
            """
            if "/home/vidar/projects/sensors/" in str(stat):
                lines.append(str(stat))
            """
            lines.append(str(stat))
        return "\n".join(lines)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    set_start_method('spawn')
    # Activating the virtual environment
    os.system("source /home/hsnews/projects/pick_n_place/venv/bin/activate")

    if TRACE_MEM:
        tracemalloc.start()
        print(snap())

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", help="Config file (JSON)", default="config.json"
    )

    args = parser.parse_args()
    cfg = parse_config(json.load(open(args.config)))
    controller = Controller(cfg)
    controller.start()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
