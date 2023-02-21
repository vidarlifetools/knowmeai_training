#!/usr/bin/env python3

import logging
from threading import Thread
import time
import socket

from dacite import from_dict
import zmq
from zmq.log.handlers import PUBHandler

debug = False
show_time = False

def get_remote_logger(name, uri, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")

    # sock = zmq.Context().socket(zmq.PUB)
    # sock.connect(uri)
    # logger.addHandler(PUBHandler(sock))
    # logger.handlers[0].setFormatter(formatter)
    # logger.propagate = False

    return logger


class Module:
    def start(self):
        self.is_running = True
        self.thread = Thread(target=self.loop)
        self.thread.start()

    def stop(self):
        self.is_running = False
        self.thread.join()

    def loop(self):
        while self.is_running:
            pass

class DataModule(Module):
    name = "default"
    config_class = None

    def __init__(self, config, status_uri, data_in_uris, data_out_uri):
        self.data_ins = []
        self.logger = get_remote_logger(self.name, status_uri)

        self.config = None

        if self.config_class:
            self.config = from_dict(data_class=self.config_class, data=config)

        if data_in_uris:
            self.data_in = zmq.Context().socket(zmq.SUB)
            self.data_in.setsockopt_string(zmq.SUBSCRIBE, "")

        for uri in data_in_uris:
            self.data_in.connect(uri)

        self.data_out = None
        if data_out_uri:
            self.data_out = zmq.Context().socket(zmq.PUB)
            self.data_out.bind(data_out_uri)

        self.recv_msg_time = time.time()

    def close_sockets(self):
        for s in self.data_ins:
            s.close()

        if self.data_out is not None:
            self.data_out.close()

    def loop(self):
        while self.is_running:
            if debug:
                print(f"Loop {self.config_class}")
            data_msg = self.recv_msgs()
            out_data = self.process_data_msg(data_msg)

            if not out_data:
                out_data = self.produce_data()

            self.send_result(out_data)

        self.close_sockets()

    def produce_data(self):
        if debug:
            print(f"Produce_data {self.config_class}")

        return None

    def send_result(self, data):
        if debug:
            print(f"Send result {self.config_class}")
        if data is None:
            return

        if show_time:
            self.logger.info(f"Produce data for {str(self.config_class).split('.')[-2]} took {time.time()-self.recv_msg_time} sec")

        if self.data_out is None:
            return
            #raise RuntimeError

        self.data_out.send_pyobj(data)

    def recv_msgs(self):
        if debug:
            print(f"Receive_msgs {self.config_class}")
        data_msg = None

        try:
            data_msg = self.data_in.recv_pyobj(zmq.NOBLOCK)
            self.recv_msg_time = time.time()
        except:
            time.sleep(0.01)

        return data_msg

    def process_data_msg(self, msg):
        if debug:
            print(f"Process_data_msg {self.config_class}")

        if not msg:
            return None