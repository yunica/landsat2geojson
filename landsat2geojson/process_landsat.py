import json
import logging

import geopandas as gpd
import numpy as np
import rasterio as rio
from geojson.feature import FeatureCollection as fc
from joblib import Parallel, delayed
from rasterio.features import shapes as rio_shape
from tqdm import tqdm

from .feature_utils import get_crs_dataset

logger = logging.getLogger("__name__")


def calculate_index_feature(scenes: list, metadata: dict, data_folder: str):
    """
    The calculate_index_feature function calculates the index with bands and metadata for each scene.

    Args:
        scenes (list): A list of scenes with features.
        metadata (list): A list of bands.
        data_folder (str): path of directory.

    Returns:
         list : A list of scenes with index data.
    """

    def calculate_index(scene_, metadata_, data_folder_):
        raw_data = scene_.get("raw_data")
        bands_ = metadata_.get("bands")
        index_name = metadata_.get("index_name")
        #  util for eval
        bands = {
            f"{i}": raw_data.get(i).get("data_read").get("out_image").astype("float32")
            for i in bands_
        }
        extra = eval(metadata_.get("extra"))

        index_result_all = eval(metadata_.get("formula"))
        index_result = np.where(index_result_all >= 0, index_result_all, 0)
        raw_data["index_result"] = index_result

        # vector
        transform_ = extra.get("transform")
        crs = get_crs_dataset(extra.get("crs"))
        crs_geojson = {
            "type": "name",
            "properties": {"name": f"urn:ogc:def:crs:EPSG::{crs}"},
        }

        mask = index_result != 0
        vector_data = []
        for (p, v) in rio_shape(
            np.round(index_result, 1), mask=mask, transform=transform_
        ):
            vector_data.append(
                {"type": "Feature", "properties": {"val": v}, "geometry": p}
            )
        raw_data["index_result_vector"] = vector_data
        if vector_data:
            raw_data["index_result_vector_4326"] = json.loads(
                gpd.GeoDataFrame.from_features(vector_data, crs=crs)
                .to_crs(crs=4326)
                .to_json()
            ).get("features", [])
        raw_data["crs"] = crs
        raw_data["crs_json"] = crs_geojson

        if data_folder and index_result.any():
            display_id = scene_.get("display_id")
            with rio.open(
                f"{data_folder_}/{display_id}__{index_name}.TIF", "w", **extra
            ) as src:
                src.write(index_result, 1)

            if vector_data:
                json.dump(
                    fc(vector_data, crs=crs_geojson),
                    open(
                        f"{data_folder_}/{display_id}__{index_name}_VECTOR.geojson", "w"
                    ),
                    indent=1,
                )

        return scene_

    scenes_index = Parallel(n_jobs=-1)(
        delayed(calculate_index)(scene, metadata, data_folder)
        for scene in tqdm(scenes, desc="calculate  MNDWI, vector")
    )
    return scenes_index
