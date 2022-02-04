import json
import click
from utils.feature_utils import (
    fc2shp,
    fc2box,
    clean_feature,
    group_by_path_row,
    minor_cloud,
)
from utils.search_metadata import fetch_sat_api
from utils.download_band import download_features
from utils.constants import WATER_BANDS


def landsar2geojson(geojson_file, data_folder, geojson_output):
    features = json.load(open(geojson_file)).get("features")
    features_shp = fc2shp(features)
    box_merge = fc2box(features_shp)
    data_query = fetch_sat_api({"bbox": box_merge.bounds}).get("features", [])
    if not data_query:
        raise Exception("No results od query")
    scenes = [clean_feature(scene) for scene in data_query]
    scenes_group = group_by_path_row(scenes)
    features_minor_cloud = [minor_cloud(scene) for scene in scenes_group.values()]
    # remove innecesary
    features_download = download_features(features_minor_cloud, WATER_BANDS, data_folder)


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
