# import time
from dataclasses import dataclass
from framework.module import DataModule
from time import sleep, time
from processes.update_files import UpdateFilesMessage
from os import listdir
from os.path import isfile, join
from datetime import datetime
import cv2
import os
import time
import pickle
import soundfile as sf
from google.cloud import storage
import multiprocessing


from sound_utils import SoundFeature
from gesture_utils import PoseFeature
from expr_utils import ExprFeature
from person_utils import PersonBbox
from feature_constants import sound_sample_rate, sound_feature_length, sound_feature_step
from constants import FaceData, SkeletonData, SoundData
import json


MODULE_UPDATE_FEATURES = "UpdateFeatures"

@dataclass
class UpdateFeaturesMessage:
    timestamp: float = 0.0
    changes: dict = None

@dataclass
class UpdateFeaturesConfig:
    name: str = ""
    run: bool = True
    google_storage_key: str = ""
    google_storage_bucket: str = ""
    destination_directory: str = ""
    models_directory: str = ""
    file_types_to_copy: list = None
    force_postprocess: bool = True
    postprocess: list = None
    display_face: bool = False
    convert_to_world: bool = False
    client: str = ""
    rec_id: str = ""
    log_directory: str = ""


class UpdateFeatures(DataModule):
    name = MODULE_UPDATE_FEATURES
    config_class = UpdateFeaturesConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.sound_feature = SoundFeature()
        self.face_feature = ExprFeature()
        self.pose_feature = PoseFeature()

    def process_data_msg(self, msg):
        if type(msg) == UpdateFilesMessage:
            self.logger.info(f"Update features started at {time.time()}")
            if self.config.run:
                # cuda_device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
                # torch.backends.cudnn.enabled = True
                log_list = {}
                # Process the videofiles and produce

                # Loop through all clients raw files and postprocess data according to settings
                dest_dict = get_dest_dict(self.config)
                for client in dest_dict.keys():
                    if self.config.client == "all" or self.config.client == client:
                        self.logger.info(f"Postprocessing recordings for: , {client}")
                        remove_files_to_be_reprocessed(client, self.config, self.logger)
                        # Check if face, skeleton and sound directories exist, create if not
                        create_feature_directories(client, self.config, self.logger)

                        # Loop through raw files
                        if self.config.rec_id == "all":
                            raw_files = [f for f in dest_dict[client]["raw"] if "_post.wav" in f]
                        else:
                            raw_files = [f for f in dest_dict[client]["raw"] if "_post.wav" in f and self.config.rec_id in f]

                        for raw_file in raw_files:
                            face_process = None
                            skeleton_process = None
                            sound_process = None
                            file_id = raw_file.split("_")[0]
                            if "face" in self.config.postprocess:
                                face_process = multiprocessing.Process(target=process_face, args=(
                                    client, file_id, dest_dict, self.config, self.logger))
                                face_process.start()
                                #self.process_face(client, file_id, dest_dict)
                            if "skeleton" in self.config.postprocess:
                                skeleton_process = multiprocessing.Process(target=process_skeleton, args=(
                                    client, file_id, dest_dict, self.config, self.logger))
                                skeleton_process.start()
                                #self.process_skeleton(client, file_id, dest_dict)
                            if "sound" in self.config.postprocess:
                                sound_annotations = get_sound_annotations(self.config, self.logger)
                                sound_process = multiprocessing.Process(target=process_sound, args=(
                                    client, file_id, dest_dict, sound_annotations, self.config, self.logger))
                                sound_process.start()
                                #self.process_sound(client, file_id, dest_dict, sound_annotations)
                            # Wait until all processes are finished
                            if face_process:
                                face_process.join()
                            if skeleton_process:
                                skeleton_process.join()
                            if sound_process:
                                sound_process.join()
                            self.logger.info(f"All feature extraction processes are finished")
                    if not self.is_running:
                        return
            return UpdateFeaturesMessage(time.time(), None)

    def stop(self):
        super().stop()

def get_dest_dict(config):
    base = config.destination_directory
    clients = [f for f in listdir(base) if not isfile(join(base, f))]
    dest_dict = {}
    for client in clients:
        dest_dict[client] = {
            "annotation": [],
            "raw": [],
            "sound": [],
            "face": [],
            "skeleton": []
        }
        dirs = [
            f
            for f in listdir(base + client + "/")
            if not isfile(join(base + client + "/", f))
        ]
        for dir in dirs:
            if dir == "raw":
                files = [
                    f
                    for f in listdir(base + client + "/raw/")
                    if isfile(join(base + client + "/raw/", f))
                ]
                for file in files:
                    if "inspected" not in file:
                        dest_dict[client]["raw"].append(file)
            if dir == "annotation":
                files = [
                    f
                    for f in listdir(base + client + "/annotation/")
                    if isfile(join(base + client + "/annotation/", f))
                ]
                for file in files:
                    if "xref" not in file:
                        dest_dict[client]["annotation"].append(file)
            if dir == "face":
                files = [f for f in listdir(base + client + "/face/")]
                for file in files:
                    dest_dict[client]["face"].append(file)
            if dir == "skeleton":
                files = [f for f in listdir(base + client + "/skeleton/")]
                for file in files:
                    dest_dict[client]["skeleton"].append(file)
            if dir == "sound":
                files = [f for f in listdir(base + client + "/sound/")]
                for file in files:
                    dest_dict[client]["sound"].append(file)

    return dest_dict


def log_changes(changes, config, logger):
    storage_client = storage.Client.from_service_account_json(
        config.google_storage_key
    )
    bucket = storage_client.bucket(config.google_storage_bucket)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    if isfile(config.destination_directory + "labels.json"):
        os.rename(
            config.destination_directory + "labels.json",
            config.destination_directory
            + "labels-"
            + timestamp
            + ".json",
        )
    if isfile(config.destination_directory + "context.json"):
        os.rename(
            config.destination_directory + "context.json",
            config.destination_directory
            + "context-"
            + timestamp
            + ".json",
        )
    cloud_file = "annotation/labels.json"
    dest_file = config.destination_directory + "labels.json"
    context_blob = bucket.blob(cloud_file)
    context_blob.download_to_filename(dest_file)
    cloud_file = "annotation/context.json"
    dest_file = config.destination_directory + "context.json"
    context_blob = bucket.blob(cloud_file)
    context_blob.download_to_filename(dest_file)

    with open(
            config.destination_directory + "changes-" + timestamp + ".json",
            "w",
    ) as outfile:
        json.dump(changes, outfile, indent=4)


def create_feature_directories(client, config, logger):
    # Check if face, skeleton and sound directories exist, create if not
    dirs = listdir(config.destination_directory + client + "/")
    for directory_name in ["face", "skeleton", "sound"]:
        if not directory_name in dirs:
            directory = os.path.join(config.destination_directory, client, directory_name)
            logger.info(f"Creating dir: {directory}")
            os.mkdir(directory)

def remove_files_to_be_reprocessed(client, config, logger):
    if config.force_postprocess:
        if config.rec_id == "all":
            # Remove skeleton, sound and face direcories
            for directory_name in ["face", "skeleton", "sound"]:
                directory = os.path.join(config.destination_directory, client, directory_name)
                if os.path.exists(directory) and directory_name in config.postprocess:
                    logger.info(f"Deleting  {directory}")
                    os.system("rm -r " + directory)
            #time.sleep(1)
        else:
            # Remove only data directories
            for directory_name in ["face", "skeleton", "sound"]:
                directory = os.path.join(config.destination_directory, client, directory_name, config.rec_id)
                if os.path.exists(directory) and directory_name in config.postprocess:
                    logger.info(f"Deleting  {directory}")
                    os.system("rm -r " + directory)

def process_face(client, file_id, dest_dict, config, logger):
    logger.info(f"Processing FACE for {client}, file id {file_id} started at {time.time()}")
    if not file_id in dest_dict[client]["face"]:
        face_feature = ExprFeature()
        face_dir = join(config.destination_directory, client, "face", file_id)
        logger.info(f"Creating: , {face_dir}")
        if not os.path.exists(face_dir):
            os.mkdir(face_dir)

        with open(join(config.destination_directory, client, "raw", file_id + ".json"), "r") as fp:
            json_info = json.load(fp)
        tracking_frame_no = 0
        if "target_person" in json_info.keys() and len(json_info["target_person"]) > 0:
            tracking = True
            tracking_bbox = json_info["target_person"]
            if "target_person_frame" in json_info.keys():
                tracking_frame_no = json_info["target_person_frame"]
        else:
            tracking = False
            tracking_bbox = []
        person = PersonBbox(tracking, tracking_bbox, tracking_frame_no)

        # Get the face image and store them frame by frame
        video_file = join(config.destination_directory, client, "raw", file_id + ".mp4")
        cap = cv2.VideoCapture(video_file)
        if not cap.isOpened():
            return
        frame_no = 0
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            person_bbox = person.detect(frame)
            if person_bbox:
                x1, y1, x2, y2 = person_bbox
                face_landmarks, valid, _ = face_feature.get(frame[y1:y2, x1:x2].copy())
                # TODO check if face image is required
                face_data = FaceData(valid, [], face_landmarks)
            else:
                face_data = FaceData(False, [], None)
            with open(join(face_dir, str(frame_no) + ".data"), "wb") as f:
                pickle.dump(face_data, f)

            frame_no += 1
    logger.info(f"Processing FACE ended at {time.time()}")

def process_skeleton(client, file_id, dest_dict, config, logger):
    logger.info(f"Processing SKELETON for {client}, file id {file_id} started at {time.time()}")
    # Generate face and skeleton frames
    if not file_id in dest_dict[client]["skeleton"]:
        pose_feature = PoseFeature()
        skeleton_dir = join(config.destination_directory, client, "skeleton", file_id)
        logger.info(f"Creating: , {skeleton_dir}")
        if not os.path.exists(skeleton_dir):
            os.mkdir(skeleton_dir)

        with open(join(config.destination_directory, client, "raw", file_id + ".json"), "r") as fp:
            json_info = json.load(fp)
        tracking_frame_no = 0
        if "target_person" in json_info.keys() and len(json_info["target_person"]) > 0:
            tracking = True
            tracking_bbox = json_info["target_person"]
            if "target_person_frame" in json_info.keys():
                tracking_frame_no = json_info["target_person_frame"]
        else:
            tracking = False
            tracking_bbox = []
        person = PersonBbox(tracking, tracking_bbox, tracking_frame_no)

        # Get the skeleton image and store them frame by frame
        video_file = join(config.destination_directory, client, "raw", file_id + ".mp4")
        cap = cv2.VideoCapture(video_file)
        if not cap.isOpened():
            return
        frame_no = 0
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            person_bbox = person.detect(frame)
            if person_bbox:
                x1, y1, x2, y2 = person_bbox
                pose_keypoints, valid, _ = pose_feature.get(frame[y1:y2, x1:x2].copy())
                skeleton_data = SkeletonData(valid, [], pose_keypoints)
                # TODO check if face image is required
            else:
                skeleton_data = SkeletonData(False, [], None)
            with open(join(skeleton_dir, str(frame_no) + ".data"), "wb") as f:
                pickle.dump(skeleton_data, f)

            frame_no += 1
    logger.info(f"Processing SKELETON ended at {time.time()}")

def get_sound_annotations(config, logger):
    sound_annotations = {}
    base = config.destination_directory
    clients = [f for f in listdir(base) if not isfile(join(base, f))]

    for client in clients:
        sound_annotations[client] = {}
        logger.info(f"Checking annotation files for: {client}")
        dirs = [
            f
            for f in listdir(base + client + "/")
            if not isfile(join(base + client + "/", f))
        ]
        for dir in dirs:
            if dir == "annotation":
                annotation_files = [
                    f
                    for f in listdir(base + client + "/annotation/")
                    if isfile(join(base + client + "/annotation/", f))
                ]
                raw_files = [
                    f
                    for f in listdir(base + client + "/raw/")
                    if isfile(join(base + client + "/raw/", f))
                ]
                for raw_file in raw_files:
                    if ".wav" in raw_file and "_post" not in raw_file:
                        sound_annotations[client][raw_file] = []
                for file in annotation_files:
                    if "xref" not in file and ".json" in file:
                        json_file = join(base + client + "/annotation/", file)
                        with open(json_file, "r") as f:
                            ann = json.load(f)
                            video_file = ann["video"].split("/")[-1]
                            if video_file not in raw_files:
                                logger.info(
                                    f"Videofile ({video_file}) not in raw files for {client}")
                            else:
                                # Save sound annotation information for this raw file
                                if ann["label_id"] >= 100:
                                    ann_data = {
                                        "label_id": ann["label_id"],
                                        "start": ann["start"],
                                        "end": ann["end"]
                                    }
                                    if ".mp4" in video_file:
                                        sound_annotations[client][
                                            video_file.replace(".mp4", ".wav")].append(ann_data)
    return sound_annotations

def process_sound(client, file_id, dest_dict, sound_annotations, config, logger):
    logger.info(f"Processing SOUND for {client}, file id {file_id} started at {time.time()}")
    if not file_id in dest_dict[client]["sound"]:
        sound_feature = SoundFeature()
        sound_dir = join(config.destination_directory, client, "sound", file_id)
        os.mkdir(sound_dir)
        # Also create client, caretaker and environmental sub directories
        for dir in ["client", "caretaker", "environment"]:
            sub_dir = os.path.join(config.destination_directory, client, "sound",
                                   file_id, dir)
            os.mkdir(sub_dir)

        # Save features for the whole recording in the sound directory
        sound_file = config.destination_directory + client + "/raw/" + file_id + ".wav"
        data, sr = sf.read(sound_file)
        print(f"Length of sound data {len(data)}, sr = {sr}")
        length = int(sound_feature_length*sound_sample_rate)
        step = int(sound_feature_step*sound_sample_rate)
        if len(data.shape)>1:
            data = data[:,0]
        for idx in range(0, len(data)-length, step):
            sound_fts = sound_feature.get_feature(data[idx:idx+length])
            start_time_ms = int(1000 * idx / sound_sample_rate)
            with open(join(sound_dir, str(start_time_ms) + ".data"), "wb") as f:
                pickle.dump(SoundData(True, sound_fts), f)

        # Save sound features for each annotated source in subdirectories
        sub_dirs = {
            101: "client",
            102: "caretaker",
            103: "environment"
        }
        for ann in sound_annotations[client][file_id + ".wav"]:
            start_sample = int(ann["start"] * sound_sample_rate)
            end_sample = int(ann["end"] * sound_sample_rate)
            for idx in range(start_sample, end_sample, step):
                sound_fts = sound_feature.get_feature(data[idx:idx+length])
                start_time_ms = int(1000 * idx / sound_sample_rate)
                with open(join(sound_dir, sub_dirs[ann["label_id"]], str(start_time_ms) + ".data"), "wb") as f:
                    pickle.dump(SoundData(True, sound_fts), f)
    logger.info(f"Processing SOUND ended at {time.time()}")


def update_features(start, stop, config, status_uri, data_in_uris, data_out_ur):
    proc = UpdateFeatures(config, status_uri, data_in_uris, data_out_ur)
    print(f"Update Features started at {time.time()}")
    while not start.is_set():
        sleep(0.1)
    proc.start()
    while not stop.is_set():
        sleep(0.1)
    proc.stop()
    print("Ending Update Features")
    sleep(0.5)
    exit()