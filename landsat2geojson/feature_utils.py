import logging
import os
from copy import deepcopy

from joblib import Parallel, delayed
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union
from tqdm import tqdm

from .constants import FEATURE_CLEAN_FIELDS

logger = logging.getLogger("__name__")


def fc2geom(features_: list):
    """
    The fc2geom function add a shapely object for each feature.

    Args:
        features_ (list): A list of dictionaries representing the features in the feature collection.

    Returns:
        list: A list of dictionaries with geometry converted from shapely.
    """

    def feature2geom(feature_: dict):
        feature_["geom"] = shape(feature_["geometry"])
        return feature_

    new_features = Parallel(n_jobs=-1)(
        delayed(feature2geom)(feature)
        for feature in tqdm(features_, desc="create shape")
    )
    return new_features


def fc2box(features_: list):
    """
    The fc2box function takes a list of features and returns a bbox.

    Args:
        features_ (list): Used to Pass a list of features to the function.
    Returns:
         bbox : A bbox of shapely.
    """

    def feature2box(feature_: dict):
        return box(*feature_["geom"].bounds)

    features_box = Parallel(n_jobs=-1)(
        delayed(feature2box)(feature)
        for feature in tqdm(features_, desc="create box from shape")
    )
    union = unary_union([*features_box])
    return box(*union.bounds)


def clean_scene(feature: dict):
    """
    The clean_scene function cleans the scene fields according to the FEATURE_CLEAN_FIELDS list.

    Args:
        feature (dict): Used to Pass a dict os escene.
    Returns:
         dict : A clean dict.
    """

    return {k: v for k, v in feature.items() if k in FEATURE_CLEAN_FIELDS}


def group_by_path_row(scenes: list):
    """
    The group_by_path_row function takes a list of scenes and returns a dictionary where the keys are path/row values
    and the values are lists of scenes with that path/row value.

    Args:
        scenes (list): list of scenes.
    Returns:
         dict : A dictionary with keys that are the path and row of each scene.
    """
    group_scenes = {}
    for scene in scenes:
        fake_key = f"{scene.get('wrs_path')}__{scene.get('wrs_row')}"
        if fake_key not in group_scenes.keys():
            group_scenes[fake_key] = []
        group_scenes[fake_key].append(scene)
    return group_scenes


def minor_cloud(scenes: list):
    """
    The minor_cloud function takes a list of scenes and returns a dictionary where the cloud cover is the low value.

    Args:
        scenes (list): list of scenes.
    Returns:
         dict : A dictionary with the escene.
    """

    if not scenes:
        return {}
    scenes_order = sorted(
        scenes, key=lambda scene: float(scene.get("cloud_cover", 100.0))
    )
    return scenes_order[0]


def create_folder(path_: str):
    """
    The create_folder function creates a folder in the specified path.

    Args:
        path_ (str): The location of the folder to be created.
    """

    if path_[:5] not in ["s3://", "gs://"] and path_:
        os.makedirs(path_, exist_ok=True)


def correct_download(feature: dict):
    """
    The correct_download function validate the feature status download.

    Args:
        feature (dict): A feature.
    Returns:
        bool : A Bool all status_download are True.
    """
    return all(
        [
            i.get("is_download")
            for i in feature.get("properties", {}).get("status_download", [])
        ]
    )


def check_geom(geom: shape):
    """
    The check_geom function validate the geom type.

    Args:
        geom (shape): The geom object.
    Returns:
        bool : A Bool if the geom is valid.
    """
    return geom.is_valid and "Polygon" in geom.geom_type


def features_in_escene(scenes: list, features_geom: list):
    """
    The features_in_escene function groups the features in the scene that intersect or include them.

    Args:
        scenes (list): list of scenes.
        features_geom (list): list of features with geom field.

    Returns:
        list: list of scenes with features.
    """

    for scene in tqdm(scenes, desc="Filter feature in scene"):
        scene_geom = scene["spatial_coverage"]
        features_contains = []
        for feature in features_geom:
            feature_geom = feature["geom"]
            is_include = feature.get("is_include", False)
            if is_include:
                continue
            #     contains
            if scene_geom.contains(feature_geom):
                feature["is_include"] = True
                feature["status"] = "contains"
                features_contains.append(deepcopy(feature))
            # intersects
            elif scene_geom.intersects(feature_geom):
                try:
                    intersection = scene_geom.intersection(feature_geom)
                    difference = feature_geom.difference(scene_geom)
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
    # remove scenes with no features
    new_scenes = [i for i in scenes if i.get("features_contains")]
    return new_scenes


def clean_path(path_: str = ""):
    """
    The clean_path function removes the last backslash from a path.

    Args:
        path_ (str): directory path.
    Returns:
        str: clean path.
    """
    if path_.endswith("/"):
        return path_[:-1]
    return path_


def url2name(url_: str = ""):
    """
    The url2name function get the name of the last directory in a path.

    Args:
        url_ (str): path.
    Returns:
        str: name.
    """
    return url_.split("/")[-1]


def get_crs_dataset(crs):
    """
    The get_crs_dataset function get the crs of a crs object.

    Args:
        crs (object): patCRS object.
    Returns:
        int: crs int value.
    """
    try:
        return int(str(crs).split(":")[1])
    except Exception as ex:
        logger.error(ex.__str__())
    return 4326


def line2polygon(features: list):
    """
    The line2polygon function converts LineString to Polygon geometry if it is possible.

    Args:
        features (list): list of dict features.
    Returns:
        list: list of dict features.
    """
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


def remove_geom(features: list):
    """
    The remove_geom function removes the geom field.

    Args:
        features (list): list of dict features.
    Returns:
        list: list of dict features.
    """
    for i in features:
        if "geom" in i.keys():
            del i["geom"]
    return features
