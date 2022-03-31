import json
import click
from utils.feature_utils import (
    fc2shp,
    fc2box,
    clean_feature,
    group_by_path_row,
    minor_cloud,
    merge_scene_features,
    remove_shp,
)
from utils.search_metadata import wraper_landsadxplore
from utils.download_band import download_scenes
from utils.process_landsat import calculate_index_feature
from utils.overpass_search import get_overpass_data
from utils.constants import QUERY_DATA
import itertools
from geojson.feature import FeatureCollection as fc


def landsat2geojson(
        username, password, geojson_file, data_folder, lansat_index, geojson_output
):
    metadata = QUERY_DATA.get(lansat_index)
    features = json.load(open(geojson_file)).get("features")
    features_shp = fc2shp(features)
    box_merge = fc2box(features_shp)
    # work api
    data_query = wraper_landsadxplore(username, password, box_merge.bounds)
    if not data_query:
        raise Exception("No results on query")
    data_result = [d for d in data_query if "LC09" in d.get("display_id")]
    data_result = group_by_path_row([clean_feature(scene) for scene in data_result])
    # remove innecesary & merge features
    data_result = merge_scene_features(
        [minor_cloud(scene) for scene in data_result.values()], features_shp
    )
    data_result = download_scenes(
        username, password, data_result, metadata.get("bands"), data_folder
    )
    # calculate mndwi
    data_result = calculate_index_feature(data_result, metadata, data_folder)
    # search osm
    minx, miny, maxx, maxy = box_merge.bounds
    osm_data = get_overpass_data(
        (miny, minx, maxy, maxx), metadata.get("query"), data_folder
    )
    # check intersects
    index_data = list(
        itertools.chain.from_iterable(
            [
                i.get("raw_data").get("index_result_vector_4326", [])
                for i in data_result
                if i.get("raw_data").get("index_result_vector_4326")
            ]
        )
    )
    index_data_shp = fc2shp(index_data)
    osm_data_sh = fc2shp(osm_data.get("features", []))
    for ind_feat in index_data_shp:
        ind_geom = ind_feat.get("geom")
        ind_feat["properties"]["source_generated"] = "landsat"
        for osm_feat in osm_data_sh:
            osm_geom = osm_feat.get("geom")
            osm_feat["properties"]["source_generated"] = "osm"
            if ind_geom.intersects(osm_geom):
                # for remove
                ind_feat["properties"]["intersects"] = True
                osm_feat["properties"]["intersects"] = True

    data_merge = remove_shp(
        [
            i
            for i in [*index_data_shp, *osm_data_sh]
            if not i.get("properties").get("intersects")
        ]
    )

    # clean shp
    json.dump(fc(data_merge), open(geojson_output, "w"), indent=2)


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
    "--geojson_file",
    help="Geojson file",
    required=True,
    type=str,
)
@click.option(
    "--data_folder",
    help="Download folder",
    type=str,
    required=False,
    default="",
)
@click.option(
    "--lansat_index",
    help="Landsar normalized index ",
    type=click.types.Choice(list(QUERY_DATA.keys())),
    default="WATER",
)
@click.option(
    "--geojson_output",
    help="Original geojson with the attributes: stile, tiles_list, tiles_bbox",
    type=str,
    default="data/supertiles.geojson",
)
def main(username, password, geojson_file, data_folder, lansat_index, geojson_output):
    landsat2geojson(
        username, password, geojson_file, data_folder, lansat_index, geojson_output
    )


if __name__ == "__main__":
    main()
