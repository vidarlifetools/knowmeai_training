# import time
import numpy as np
from dataclasses import dataclass
from framework.module import DataModule
from time import sleep
from processes.update_files import UpdateFilesMessage
from processes.update_features import UpdateFeaturesMessage
import cv2
import time

MODULE_LABEL = "Label"

@dataclass
class LabelMessage:
    name: str = ""

@dataclass
class LabelConfig:
    name: str = ""

class Label(DataModule):
    name = MODULE_LABEL
    config_class = LabelConfig

    def __init__(self, *args):
        super().__init__(*args)

    def process_data_msg(self, msg):
        if type(msg) == UpdateFeaturesMessage:
            time.sleep(5)
            return LabelMessage("Label messaghe")
        return None


def label(start, stop, config, status_uri, data_in_uris, data_out_ur):
    proc = Label(config, status_uri, data_in_uris, data_out_ur)
    print(f"Label started at {time.time()}")
    while not start.is_set():
        sleep(0.1)
    proc.start()
    while not stop.is_set():
        sleep(0.1)
    proc.stop()
    print("Ending label")
    sleep(0.5)
    exit()