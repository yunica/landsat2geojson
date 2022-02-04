import json
import click
from utils.feature_utils import (
    fc2shp,
    fc2box,
    clean_feature,
    group_by_path_row,
    minor_cloud,
    correct_download,
    merge_scene_features,
)
from utils.search_metadata import fetch_sat_api
from utils.download_band import download_scenes
from utils.constants import WATER_BANDS
from utils.process_landsat import clip_features, calculate_mndwi_feature


def landsar2geojson(geojson_file, data_folder, geojson_output):
    features = json.load(open(geojson_file)).get("features")
    features_shp = fc2shp(features)
    box_merge = fc2box(features_shp)
    # work api
    data_query = fetch_sat_api({"bbox": box_merge.bounds}).get("features", [])
    if not data_query:
        raise Exception("No results on query")

    scenes = [clean_feature(scene) for scene in data_query]
    scenes_group = group_by_path_row(scenes)
    scenes_minor_cloud = [minor_cloud(scene) for scene in scenes_group.values()]
    # remove innecesary & merge features
    scenes_minor_cloud_merge = merge_scene_features(scenes_minor_cloud, features_shp)
    scenes_download = download_scenes(
        scenes_minor_cloud_merge, WATER_BANDS, data_folder
    )
    # filter only download
    scenes_correct_download = [
        feature for feature in scenes_download if correct_download(feature)
    ]
    # works by feature
    clip_features(scenes_correct_download)
    calculate_mndwi_feature(scenes_correct_download)


@click.command(short_help="Script to extract features from landsat")
@click.option(
    "--geojson_file",
    help="Geojson file",
    required=True,
    type=str,
)
@click.option(
    "--data_folder",
    help="Download folder",
    type=str,
    default="data",
)
@click.option(
    "--geojson_output",
    help="Original geojson with the attributes: stile, tiles_list, tiles_bbox",
    type=str,
    default="data/supertiles.geojson",
)
def main(
    geojson_file,
    data_folder,
    geojson_output,
):
    landsar2geojson(geojson_file, data_folder, geojson_output)


if __name__ == "__main__":
    main()
