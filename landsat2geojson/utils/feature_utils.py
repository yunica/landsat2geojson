import os
from shapely.geometry import shape, box
from shapely.ops import unary_union
from tqdm import tqdm
from joblib import Parallel, delayed
from .constants import FEATURE_CLEAN_FIELDS
from copy import deepcopy
import logging

logger = logging.getLogger("__name__")


def fc2shp(features_):
    def feature2shp(feature_):
        feature_["geom"] = shape(feature_["geometry"])
        return feature_

    new_features = Parallel(n_jobs=-1)(
        delayed(feature2shp)(feature)
        for feature in tqdm(features_, desc="create shape")
    )
    return new_features


def fc2box(features_):
    def feature2box(feature_):
        return box(*feature_["geom"].bounds)

    feature_box = Parallel(n_jobs=-1)(
        delayed(feature2box)(feature)
        for feature in tqdm(features_, desc="create box from shape")
    )
    union = unary_union([*feature_box])
    return box(*union.bounds)


def clean_feature(feature):
    return {k: v for k, v in feature.items() if k in FEATURE_CLEAN_FIELDS}


def group_by_path_row(scenes):
    group_scenes = {}
    for scene in scenes:
        fake_key = f"{scene.get('wrs_path')}__{scene.get('wrs_row')}"
        if fake_key not in group_scenes.keys():
            group_scenes[fake_key] = []
        group_scenes[fake_key].append(scene)
    return group_scenes


def minor_cloud(scenes_list):
    if not scenes_list:
        return {}
    scenes_order = sorted(scenes_list, key=lambda scene: scene.get("cloud_cover"))
    return scenes_order[0]


def create_folder(path_):
    if path_[:5] not in ["s3://", "gs://"] and path_:
        os.makedirs(path_, exist_ok=True)


def correct_download(feature):
    return all(
        [
            i.get("is_download")
            for i in feature.get("properties", {}).get("status_download", [])
        ]
    )


def merge_scene_features(scenes, features_shp):
    # compile features include
    for scene in scenes:
        scene_shp = scene["spatial_coverage"]
        features_contains = []
        for feature in features_shp:
            feature_shp = feature["geom"]
            if scene_shp.contains(feature_shp) and not feature.get("is_include"):
                features_contains.append(deepcopy(feature))
                feature["is_include"] = True
        scene["features_contains"] = features_contains
        return scenes


def clean_path(path_=""):
    if path_.endswith("/"):
        return path_[:-1]
    return path_


def url2name(url_=""):
    return url_.split("/")[-1]


def get_crs_dataset(crs):
    try:
        return int(str(crs).split(":")[1])
    except Exception as ex:
        logger.error(ex.__str__())
    return 4326
