import os
from shapely.geometry import shape, box, mapping
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


def check_geom(geom):
    return geom.is_valid and "Polygon" in geom.geom_type


def merge_scene_features(scenes, features_shp):
    # compile features include
    new_scenes = []
    for scene in scenes:
        scene_shp = scene["spatial_coverage"]
        features_contains = []
        for feature in features_shp:
            feature_shp = feature["geom"]
            if not feature.get("is_include"):
                if scene_shp.contains(feature_shp):
                    feature["is_include"] = True
                    feature["status"] = "contains"
                    features_contains.append(deepcopy(feature))
                elif scene_shp.intersects(feature_shp):
                    try:
                        intersection = scene_shp.intersection(feature_shp)
                        difference = feature_shp.difference(scene_shp)
                        feature["status"] = "intersects"
                        if check_geom(intersection):
                            new_feature = deepcopy(feature)
                            new_feature["geometry"] = mapping(intersection)
                            features_contains.append(new_feature)
                        if check_geom(difference):
                            feature["geometry"] = mapping(difference)
                        else:
                            feature["is_include"] = True
                    except Exception as ex:
                        logger.error(ex.__str__())
        scene["features_contains"] = features_contains
        # remove scenes no features
        if features_contains:
            new_scenes.append(scene)
    return new_scenes


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


def line2polygon(features):
    for feature in features:
        sha = shape(feature.get("geometry"))
        if "Line" in sha.geom_type and sha.is_closed:
            feature["geometry"][
                "type"
            ] = f'{feature["geometry"]["type"].replace("LineString", "Polygon")}'
            feature["geometry"]["coordinates"] = [
                feature["geometry"]["coordinates"],
            ]
    return features


def remove_shp(features):
    for i in features:
        if 'geom' in i.keys():
            del i['geom']
    return features
