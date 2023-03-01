# import time
from os import listdir, mkdir
from os.path import isfile, join
import json

def eval_sound_annotations(base, client):
    # Create a dict with all annotations sparated in sound and expr
    mixed_annotations = {}
    dir = join(base, client, "annotation")
    files = [
        f
        for f in listdir(dir)
        if isfile(join(dir, f))
    ]
    for file in files:
        if "xref" not in file and ".json" in file:
            json_file = join(base + client + "/annotation/", file)
            with open(json_file, "r") as f:
                ann = json.load(f)
                video_file = ann["video"].split("/")[-1]
                if video_file not in mixed_annotations.keys():
                    a = {
                        "expr": [],
                        "sound": []
                    }
                    mixed_annotations[video_file] = a
                else:
                    # Save sound annotation information for this raw file
                    ann_data = {
                        "label_name": ann["label_name"],
                        "start": ann["start"],
                        "end": ann["end"]
                    }
                    if ann["label_id"] >= 100:
                        mixed_annotations[video_file]["sound"].append(ann_data)
                    else:
                        mixed_annotations[video_file]["expr"].append(ann_data)
    # Check ovelapping annotations
    overlapping = {
        "unannotated": 0.0
    }
    only_sound = {
        "unannotated": 0.0
    }
    only_expr = {
        "unannotated": 0.0
    }
    for video in mixed_annotations.keys():
        for sound_anno in mixed_annotations[video]["sound"]:
            for expr_anno in mixed_annotations[video]["expr"]:
                label = expr_anno["label_name"]
                if not label in overlapping.keys():
                    overlapping[label] = 0.0
                    only_sound[label] = 0.0
                    only_expr[label] = 0.0
                if sound_anno["start"]<=expr_anno["end"] and sound_anno["end"]>=expr_anno["start"]:
                    if sound_anno["start"]>=expr_anno["start"]:
                        start_t = sound_anno["start"]
                        only_expr[label] += sound_anno["start"] - expr_anno["start"]
                    else:
                        start_t = expr_anno["start"]
                        only_sound["unannotated"] += expr_anno["start"] - sound_anno["start"]
                    if sound_anno["end"] >= expr_anno["end"]:
                        end_t = expr_anno["end"]
                        only_sound["unannotated"] += sound_anno["end"]-expr_anno["end"]
                    else:
                        end_t = sound_anno["end"]
                        only_expr[label] += expr_anno["end"] - sound_anno["end"]
                    overlapping[label] += end_t - start_t
                else:
                    only_sound["unannotated"] += sound_anno["end"] - sound_anno["start"]
                    only_expr[label] += expr_anno["end"] - expr_anno["start"]
    return overlapping, only_sound, only_expr