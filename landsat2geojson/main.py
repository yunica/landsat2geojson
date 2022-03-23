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


def landsar2geojson(username, password, geojson_file, data_folder, geojson_output):
    features = json.load(open(geojson_file)).get("features")
    features_shp = fc2shp(features)
    box_merge = fc2box(features_shp)
    # work api
    data_query = wraper_landsadxplore(username, password, box_merge.bounds)
    if not data_query:
        raise Exception("No results on query")
    filter_data = [d for d in data_query if "LC09" in d.get("display_id")]
    scenes = [clean_feature(scene) for scene in filter_data]
    scenes_group = group_by_path_row(scenes)
    scenes_minor_cloud = [minor_cloud(scene) for scene in scenes_group.values()]
    # remove innecesary & merge features
    scenes_minor_cloud_merge = merge_scene_features(scenes_minor_cloud, features_shp)
    scenes_download_clip = download_scenes(
        username, password, scenes_minor_cloud_merge, data_folder
    )
    # calculate mndwi
    scenes_mndwi = calculate_mndwi_feature(scenes_download_clip)
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
    landsar2geojson(username, password, geojson_file, data_folder, geojson_output)


if __name__ == "__main__":
    main()
