import os
from shapely.geometry import mapping, shape, box, Polygon
from shapely.ops import unary_union
from tqdm import tqdm
from joblib import Parallel, delayed
from .constants import FEATURE_CLEAN_FIELDS
from copy import deepcopy


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


def group_by_path_row(features):
    new_features = {}
    for feature_ in features:
        props = feature_.get("properties")
        fake_key = (
            f"{props.get('landsat:wrs_path', '')}__{props.get('landsat:wrs_row', '')}"
        )
        if fake_key not in new_features.keys():
            new_features[fake_key] = []
        new_features[fake_key].append(feature_)
    return new_features


def minor_cloud(feature_list):
    if not feature_list:
        return {}
    sub_features = sorted(
        feature_list, key=lambda feat: feat["properties"]["eo:cloud_cover"]
    )
    return sub_features[0]


def create_folder(path_):
    if path_[:5] not in ["s3://", "gs://"]:
        os.makedirs(path_, exist_ok=True)


def correct_download(feature):
    return all(
        [
            i.get("is_download")
            for i in feature.get("properties", {}).get("status_download", [])
        ]
    )


def merge_scene_features(scenes, features_shp):
    scenes_shp = fc2shp(scenes)

    # compile features include
    for scene in scenes_shp:
        scene_shp = scene["geom"]
        features_contains = []
        for feature in features_shp:
            feature_shp = feature["geom"]
            if scene_shp.contains(feature_shp) and not feature.get("is_include"):
                features_contains.append(deepcopy(feature))
                feature["is_include"] = True
        scene["features_contains"] = features_contains
        return scenes_shp
