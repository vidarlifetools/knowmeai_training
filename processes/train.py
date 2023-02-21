# import time
import numpy as np
from dataclasses import dataclass
from framework.module import DataModule
from time import sleep
from processes.label import LabelMessage
import cv2
import time

MODULE_TRAIN = "Train"

@dataclass
class TrainMessage:
    name: str = ""

@dataclass
class TrainConfig:
    name: str = ""


class Train(DataModule):
    name = MODULE_TRAIN
    config_class = TrainConfig

    def __init__(self, *args):
        super().__init__(*args)

    def process_data_msg(self, msg):
        if type(msg) == LabelMessage:
            time.sleep(5)
            return TrainMessage("Label messaghe")
        return None


def train(start, stop, config, status_uri, data_in_uris, data_out_ur):
    proc = Train(config, status_uri, data_in_uris, data_out_ur)
    print(f"Train started at {time.time()}")
    while not start.is_set():
        sleep(0.1)
    proc.start()
    while not stop.is_set():
        sleep(0.1)
    proc.stop()
    print("Ending Train")
    sleep(0.5)
    exit()