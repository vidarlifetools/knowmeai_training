# import time
import numpy as np
from dataclasses import dataclass
from framework.module import DataModule
from time import sleep, time
from processes.update_features import UpdateFeaturesMessage
from os.path import join
from os import listdir
import pickle
from constants import SoundData, SkeletonData

MODULE_COMPARE = "Compare"

@dataclass
class CompareMessage:
    name: str = ""

@dataclass
class CompareConfig:
    name: str = ""
    compare_data_types: list = None
    source_directory: str = ""
    facit_directory: str = ""
    client: str = ""
    rec_id: str = ""


class Compare(DataModule):
    name = MODULE_COMPARE
    config_class = CompareConfig

    def __init__(self, *args):
        super().__init__(*args)

    def process_data_msg(self, msg):
        if type(msg) == UpdateFeaturesMessage:
            self.logger.info(f"Comparing {self.config.rec_id} for {self.config.client}")
            for data_type in self.config.compare_data_types:
                self.logger.info(f"Comparing {data_type}")
                if data_type == "face":
                    source_dir = join(self.config.source_directory,
                                   self.config.client,
                                   "face", self.config.rec_id)
                    facit_dir = join(self.config.facit_directory,
                                   self.config.client,
                                   "face", self.config.rec_id)
                    source_files = listdir(source_dir)
                    facit_files = listdir(facit_dir)
                    self.logger.info(f"Source has {len(source_files)} and facit contains {len(facit_files)} files")
                    valid_found = False
                    for file in facit_files:
                        source_file = join(self.config.source_directory,
                                       self.config.client,
                                       "face", self.config.rec_id, str(source_files[0]))
                        facit_file = join(self.config.facit_directory,
                                       self.config.client,
                                       "face", self.config.rec_id, str(facit_files[0]))
                        with open(facit_file, "rb") as fp:
                            facit = pickle.load(fp)
                        with open(source_file, "rb") as fp:
                            source = pickle.load(fp)
                        if (source.valid or facit.valid) and not valid_found:
                            valid_found = True
                            # Compare Landmarks
                            for i in range(min(len(source.landmarks), len(facit.landmarks))):
                                s = source.landmarks[i]
                                f = facit.landmarks[i]
                                print(f"Landmark {i} Source: {s[0]:0.2f}, {s[1]:0.2f}, {s[2]:0.2f}"
                                      f" Facit:{f[0]:0.2f}, {f[1]:0.2f}, {f[2]:0.2f}")
                         #self.logger.info(f"Source: {source.face}/{len(source.landmarks)}, Facit: {len(facit.face)}/{len(facit.landmarks)}")
                if data_type == "skeleton":
                    source_dir = join(self.config.source_directory,
                                      self.config.client,
                                      "skeleton", self.config.rec_id)
                    facit_dir = join(self.config.facit_directory,
                                     self.config.client,
                                     "skeleton", self.config.rec_id)
                    source_files = listdir(source_dir)
                    facit_files = listdir(facit_dir)
                    self.logger.info(f"Source has {len(source_files)} and facit contains {len(facit_files)} files")
                    valid_found = False
                    for file in facit_files:
                        source_file = join(self.config.source_directory,
                                           self.config.client,
                                           "skeleton", self.config.rec_id, str(source_files[0]))
                        facit_file = join(self.config.facit_directory,
                                          self.config.client,
                                          "skeleton", self.config.rec_id, str(facit_files[0]))
                        with open(facit_file, "rb") as fp:
                            facit = pickle.load(fp)
                        with open(source_file, "rb") as fp:
                            source = pickle.load(fp)
                        if (source.valid or facit.valid) and not valid_found:
                            valid_found = True
                            # Compare Landmarks
                            for i in range(min(len(source.skeleton), len(facit.skeleton))):
                                s = source.skeleton[i]
                                f = facit.skeleton[i]
                                print(f"Keypoints {i} Source: {s[0]:0.2f}, {s[1]:0.2f}, {s[2]:0.2f}"
                                      f" Facit:{f[0]:0.2f}, {f[1]:0.2f}, {f[2]:0.2f}")
                        # self.logger.info(f"Source: {source.face}/{len(source.landmarks)}, Facit: {len(facit.face)}/{len(facit.landmarks)}")
        return None


def compare(start, stop, config, status_uri, data_in_uris, data_out_ur):
    proc = Compare(config, status_uri, data_in_uris, data_out_ur)
    print(f"Compare started at {time()}")
    while not start.is_set():
        sleep(0.1)
    proc.start()
    while not stop.is_set():
        sleep(0.1)
    proc.stop()
    print("Ending Compare")
    sleep(0.5)
    exit()