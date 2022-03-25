import json
import click
from utils.feature_utils import (
    fc2shp,
    fc2box,
    clean_feature,
    group_by_path_row,
    minor_cloud,
    merge_scene_features,
)
from utils.search_metadata import wraper_landsadxplore
from utils.download_band import download_scenes
from utils.process_landsat import calculate_mndwi_feature


def landsat2geojson(username, password, geojson_file, data_folder, geojson_output):
    features = json.load(open(geojson_file)).get("features")
    features_shp = fc2shp(features)
    box_merge = fc2box(features_shp)
    # work api
    data_query = wraper_landsadxplore(username, password, box_merge.bounds)
    if not data_query:
        raise Exception("No results on query")
    dara_result = [d for d in data_query if "LC09" in d.get("display_id")]
    dara_result = [clean_feature(scene) for scene in dara_result]
    dara_result = group_by_path_row(dara_result)
    dara_result = [minor_cloud(scene) for scene in dara_result.values()]
    # remove innecesary & merge features
    dara_result = merge_scene_features(dara_result, features_shp)
    dara_result = download_scenes(username, password, dara_result, data_folder)
    # calculate mndwi
    dara_result = calculate_mndwi_feature(dara_result, data_folder)
    # search osm


@click.command(short_help="Script to extract features from landsat")
@click.option(
    "-u",
    "--username",
    type=str,
    help="EarthExplorer username.",
    envvar="LANDSATXPLORE_USERNAME",
)
@click.option(
    "-p",
    "--password",
    type=str,
    help="EarthExplorer password.",
    envvar="LANDSATXPLORE_PASSWORD",
)
@click.option(
    "--geojson_file", help="Geojson file", required=True, type=str,
)
@click.option(
    "--data_folder", help="Download folder", type=str, required=False, default="",
)
@click.option(
    "--geojson_output",
    help="Original geojson with the attributes: stile, tiles_list, tiles_bbox",
    type=str,
    default="data/supertiles.geojson",
)
def main(
    username, password, geojson_file, data_folder, geojson_output,
):
    landsat2geojson(username, password, geojson_file, data_folder, geojson_output)


if __name__ == "__main__":
    main()
