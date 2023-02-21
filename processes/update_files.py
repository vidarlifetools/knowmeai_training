# import time
from dataclasses import dataclass
from framework.module import DataModule
from time import sleep, time
from os import listdir, mkdir
import os
from os.path import isfile, join
from google.cloud import storage
from datetime import datetime
import json


MODULE_UPDATE_FILES = "UpdateFiles"

@dataclass
class UpdateFilesMessage:
    timestamp: float = 0.0
    changes: dict = None


@dataclass
class UpdateFilesConfig:
    name:  str = "UpdateFiles"
    run: bool = True
    google_storage_key:  str = "/home/vidar/projects/knowmeai/sensors/KnowMeAI-326518239433.json"
    google_storage_bucket:  str = "knowmeai_bucket",
    destination_directory:  str = "/home/vidar/projects/test_annotation_data/"
    file_types_to_copy:  list = None
    save_unannotated:  bool = False
    client:  str = "ahmed"
    rec_id:  str = "all"
    log_directory:  str = "/home/vidar/projects/tmp"
    update_interval_hours: int = 0


class UpdateFiles(DataModule):
    name = MODULE_UPDATE_FILES
    config_class = UpdateFilesConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.storage_client = storage.Client.from_service_account_json(
                self.config.google_storage_key
            )
        self.bucket = self.storage_client.bucket(self.config.google_storage_bucket)

    def loop(self):
        while self.is_running:
            changes = {}
            if self.config.run:
                # Update local files to match Google storage files
                cloud_dict = self.get_cloud_dict(self.config.client)
                dest_dict = self.get_dest_dict(self.config.client)
                for client in cloud_dict.keys():
                    if client == self.config.client or self.config.client == "all":
                        found = [f for f in dest_dict.keys() if client == f]
                        if not found:
                            # Create the directory structure for new client
                            self.logger.info(f"Creating directory structure for {client}")
                            mkdir(self.config.destination_directory + client)
                            mkdir(self.config.destination_directory + client + "/raw")
                            mkdir(
                                self.config.destination_directory + client + "/annotation"
                            )
                            # Add the new client to dest_clients. Since empty, all files will be copied
                            dest_dict["client"] = {"raw": [], "annotation": []}

                        added_files, deleted_files = self.add_remove_annotation_files(client, cloud_dict,
                                                                                              dest_dict)
                        changes["annotation_files"] = {client: [{"deleted": deleted_files, "added": added_files}]}
                        added_files, deleted_files = self.add_remove_raw_files(client, cloud_dict, dest_dict)
                        changes["raw_files"] = {client: [{"deleted": deleted_files, "added": added_files}]}

                if changes["annotation_files"].keys() or changes["raw_files"].keys():
                    self.log_changes(changes)
            self.send_result(UpdateFilesMessage(time(), changes))
            if self.config.update_interval_hours != 0:
                sleep(self.config.update_interval_hours*3600)
            else:
                break
        self.close_sockets()


    def stop(self):
        super().stop()

    def get_cloud_dict(self, client):
        self.logger.info(f"Compiling a list of files located on google storage")
        cloud_dict = {}
       # Find client names
        blobs = self.storage_client.list_blobs(self.config.google_storage_bucket)
        for blob in blobs:
            # Assume that the directory is listed before directory content!

            blob_name_split = blob.name.split("/")
            # If a directory only, pick the client name
            if len(blob_name_split) == 3 and blob_name_split[2] == "":
                if client == "all" or client == blob_name_split[1]:
                    cl = {
                        "annotation": [],
                        "raw": []
                    }
                    cloud_dict[blob_name_split[1]] = cl
            """
            blobs = self.storage_client.list_blobs(self.config.google_storage_bucket)
            #bucket = self.storage_client.bucket("knowmeai_bucket")
            for blob in blobs:
            """
            # Get annotation and raw files
            if (
                len(blob_name_split) == 4
                and blob_name_split[1] in cloud_dict.keys()
                and blob_name_split[3] != ""
                and blob_name_split[3] != "inspected.json"
                and blob_name_split[3] != "xref.json"
            ):
                if client == "all" or client == blob_name_split[1]:
                    """
                    # Check if file is empty
                    blob_data = self.bucket.get_blob(blob.name)
                    if blob_data != None:
                        if blob_data.size == 0:
                            empty[clients.index(split[1])].append(split[3])
                            self.logger.info(f"Empty file: {blob.name}")
                    """
                    if blob_name_split[2] == "raw":
                        save_file = False
                        for string in self.config.file_types_to_copy:
                            if string in blob_name_split[3]:
                                save_file = True
                        if save_file:
                            cloud_dict[blob_name_split[1]]["raw"].append(blob_name_split[3])
                    if blob_name_split[2] == "annotation":
                        cloud_dict[blob_name_split[1]]["annotation"].append(blob_name_split[3])

        # Check if all files are present and, remove from list if not
        for cloud_client in cloud_dict.keys():
            self.logger.info(f"Checking if all files are present and non are empty")
            if client == cloud_client or client == "all":
                delete = {}
                for raw_file in cloud_dict[client]["raw"]:
                    rec_id = raw_file.split(".")[0]
                    rec_id = rec_id.replace("_post", "")
                    rec_id = rec_id.replace("_imu", "")
                    rec_id = rec_id.replace("_depth", "")
                    if rec_id + ".mp4" in cloud_dict[client]["raw"]\
                        and rec_id + ".wav" in cloud_dict[client]["raw"]\
                        and rec_id + ".json" in cloud_dict[client]["raw"]\
                        and rec_id + "_post.wav" in cloud_dict[client]["raw"]:
                        pass
                    else:
                        self.logger.info(f"Removing incomplete file {raw_file} from {client}")
                        if client not in delete.keys():
                            delete[client] = []
                        delete[client].append(raw_file)
            """
            # Check for empty files
            self.logger.info(f"Remove all recorded files in case one of the files is empty")
            for i, empty_files in enumerate(empty):
                for empty_file in empty_files:
                    rec_id = empty_file.split(".")[0]
                    rec_id = rec_id.replace("_depth", "")
                    rec_id = rec_id.replace("_post", "")
                    delete[i].append(rec_id + ".json")
                    delete[i].append(rec_id + ".mp4")
                    delete[i].append(rec_id + ".wav")
                    delete[i].append(rec_id + "_post.wav")
                    delete[i].append(rec_id + "_depth.mp4")
                    delete[i].append(rec_id + ".npy.gz")
            """
        # Remove uncomplete and/or empty files
        for del_client in delete.keys():
            for del_file in delete[del_client]:
                if del_file in cloud_dict[del_client]["raw"]:
                    cloud_dict[del_client]["raw"].remove(del_file)
        return cloud_dict

    def get_dest_dict(self, client):
        base = self.config.destination_directory
        dest_clients = [f for f in listdir(base) if not isfile(join(base, f))]
        dest_dict = {}
        for dest_client in dest_clients:
            if client == "all" or client == dest_client:
                dest_dict[dest_client] = {
                    "annotation": [],
                    "raw": []
                }
                dirs = [
                    f
                    for f in listdir(base + dest_client + "/")
                    if not isfile(join(base + dest_client + "/", f))
                ]
                for dir in dirs:
                    if dir == "raw":
                        files = [
                            f
                            for f in listdir(base + dest_client + "/raw/")
                            if isfile(join(base + dest_client + "/raw/", f))
                        ]
                        for file in files:
                            if "inspected" not in file:
                                dest_dict[dest_client]["raw"].append(file)
                    if dir == "annotation":
                        files = [
                            f
                            for f in listdir(base + dest_client + "/annotation/")
                            if isfile(join(base + dest_client + "/annotation/", f))
                        ]
                        for file in files:
                            if "xref" not in file:
                                dest_dict[dest_client]["annotation"].append(file)
        return dest_dict

    def add_remove_annotation_files(self,client, cloud_dict, dest_dict):
        deleted_files = {}
        added_files = {}
        # Check if there are files to be deleted from the annotation directory
        for dest_client in dest_dict:
            deleted_files[dest_client] = []
            added_files[dest_client] = []
            if len(dest_dict[dest_client]["annotation"]) > 0:
                for ann in dest_dict[dest_client]["annotation"]:
                    if ann not in cloud_dict[dest_client]["annotation"]:
                        self.logger.info(f"Removing: {ann} from {dest_client}")
                        os.remove(
                            self.config.destination_directory
                            + dest_client
                            + "/annotation/"
                            + ann
                        )
                        deleted_files[dest_client].append("/annotation/" + ann)
            # Check if there are files to be added to the annotation directory
            self.logger.info(f"Checking if there are files to be added to the annotation directory")
            if len(cloud_dict[dest_client]["annotation"]) > 0:
                for i, ann in enumerate(cloud_dict[dest_client]["annotation"]):
                    if ann not in dest_dict[dest_client]["annotation"]:
                        self.logger.info(f"Adding: {ann} to client: {dest_client}")
                        cloud_file = (
                                "annotation/" + dest_client + "/annotation/" + ann
                        )
                        dest_file = (
                                self.config.destination_directory
                                + dest_client
                                + "/annotation/"
                                + ann
                        )
                        context_blob = self.bucket.blob(cloud_file)
                        context_blob.download_to_filename(dest_file)
                        added_files[dest_client].append("/annotation/" + ann)

        return added_files, deleted_files

    def add_remove_raw_files(self, client, cloud_dict, dest_dict):
        self.logger.info("Create a list of active raw files referenced in the annotation files")
        deleted_files = []
        added_files = []
        annotated_file = []
        for ann_file in cloud_dict[client]["annotation"]:
            json_file = (
                    self.config.destination_directory
                    + client
                    + "/annotation/"
                    + ann_file
            )
            with open(json_file, mode="r+") as jsonFile:
                annotation = json.load(jsonFile)
                base_file = annotation["video"].split("/")[-1].split(".")[0]
                annotated_file.append(base_file + ".json")
                annotated_file.append(base_file + ".mp4")
                annotated_file.append(base_file + ".wav")
                annotated_file.append(base_file + "_post.wav")
                annotated_file.append(base_file + "_depth.mp4")
                annotated_file.append(base_file + ".npy.gz")
                annotated_file.append(base_file + "_imu.dat")

        # Check if there are files to be deleted from the raw directory, not referenced in a annotation file
        self.logger.info("Check if there are files to be deleted from the raw director")
        if len(dest_dict[client]["raw"]):
            for dest_raw_file in dest_dict[client]["raw"]:
                if dest_raw_file not in cloud_dict[client]["raw"] or (dest_raw_file not in annotated_file and not self.config.save_unannotated):
                    self.logger.info(f"Removing: {dest_raw_file} from client: {client}")
                    os.remove(
                        self.config.destination_directory
                        + client
                        + "/raw/"
                        + dest_raw_file
                    )
                    deleted_files.append("/raw/" + dest_raw_file)

        # Check if there are files to be added to the raw directory
        self.logger.info(f"Check if there are files to be added from the raw director")
        if len(cloud_dict[client]["raw"]):
            for dest_raw_file in cloud_dict[client]["raw"]:
                if dest_raw_file not in dest_dict[client]["raw"] and (dest_raw_file in annotated_file or self.config.save_unannotated) \
                        and (self.config.rec_id == "all" or self.config.rec_id in dest_raw_file):
                    self.logger.info(f"Adding: {dest_raw_file} to client: {client}")
                    cloud_file = "annotation/" + client + "/raw/" + dest_raw_file
                    # Check if file type is to be stored
                    for string in self.config.file_types_to_copy:
                        if string in dest_raw_file and "_depth" not in dest_raw_file:
                            dest_file = (
                                    self.config.destination_directory
                                    + client
                                    + "/raw/"
                                    + dest_raw_file
                            )
                            context_blob = self.bucket.blob(cloud_file)
                            context_blob.download_to_filename(dest_file)
                            added_files.append("/raw/" + dest_raw_file)
        return added_files, deleted_files

    def log_changes(self, changes):
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if isfile(self.config.destination_directory + "labels.json"):
            os.rename(
                self.config.destination_directory + "labels.json",
                self.config.destination_directory
                + "labels-"
                + timestamp
                + ".json",
            )
        if isfile(self.config.destination_directory + "context.json"):
            os.rename(
                self.config.destination_directory + "context.json",
                self.config.destination_directory
                + "context-"
                + timestamp
                + ".json",
            )
        cloud_file = "annotation/labels.json"
        dest_file = self.config.destination_directory + "labels.json"
        context_blob = self.bucket.blob(cloud_file)
        context_blob.download_to_filename(dest_file)
        cloud_file = "annotation/context.json"
        dest_file = self.config.destination_directory + "context.json"
        context_blob = self.bucket.blob(cloud_file)
        context_blob.download_to_filename(dest_file)

        with open(
                self.config.destination_directory + "changes-" + timestamp + ".json",
                "w",
        ) as outfile:
            json.dump(changes, outfile, indent=4)

    def remove_unreferenced_raw_files(self):
        # Check that all annotation files has an existing raw file
        if self.config.save_unannotated:
            return
        else:
            sound_annotations = {}
            base = self.config.destination_directory
            clients = [f for f in listdir(base) if not isfile(join(base, f))]

            for client in clients:
                sound_annotations[client] = {}
                self.logger.info(f"Checking annotation files for: {client}")
                dirs = [
                    f
                    for f in listdir(base + client + "/")
                    if not isfile(join(base + client + "/", f))
                ]
                for dir in dirs:
                    if dir == "annotation":
                        files = [
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
                            if "_post" in raw_file:
                                sound_annotations[client][raw_file.replace("_post.wav", ".wav")] = []
                        for file in files:
                            if "xref" not in file and ".json" in file:
                                json_file = join(base + client + "/annotation/", file)
                                found = False
                                with open(json_file, "r") as f:
                                    ann = json.load(f)
                                    video_file = ann["video"].split("/")[-1]
                                    if video_file not in raw_files:
                                        found = True
                                        self.logger.info(f"Removing annotation file : {json_file} from client: {client}")
                                    else:
                                        # Save sound annotation information for this raw file
                                        if ann["label_id"] >= 100:
                                            ann_data = {
                                                "label_id": ann["label_id"],
                                                "start": ann["start"],
                                                "end": ann["end"]
                                            }
                                            if ".mp4" in video_file:
                                                sound_annotations[client][video_file.replace(".mp4", ".wav")].append(ann_data)
                                if found:
                                    os.remove( json_file )



    def create_feature_directories(self, client):
        # Check if face, skeleton and sound directories exist, create if not
        dirs = listdir(self.config.destination_directory + client + "/")
        for directory_name in ["face", "skeleton", "sound"]:
            if not directory_name in dirs:
                directory = os.path.join(self.config.destination_directory, client, directory_name)
                self.logger.info(f"Creating dir: {directory}")
                os.mkdir(directory)
        time.sleep(1)



def update_files(start, stop, config, status_uri, data_in_uris, data_out_ur):
    proc = UpdateFiles(config, status_uri, data_in_uris, data_out_ur)
    print(f"Update Files started at {time()}")
    while not start.is_set():
        sleep(0.1)
    proc.start()
    while not stop.is_set():
        sleep(0.1)
    proc.stop()
    print("Ending Update Files")
    sleep(0.5)
    exit()