from tqdm import tqdm
from joblib import Parallel, delayed
import logging
import rasterio as rio
import numpy as np
from rasterio.features import shapes as rio_shape
import json
from geojson.feature import FeatureCollection as fc
from .feature_utils import get_crs_dataset

logger = logging.getLogger("__name__")


def calculate_index_feature(scenes, metadata, data_folder):
    def calculate_index(scene_, metadata_, data_folder_):
        raw_data = scene_.get("raw_data")
        bands_ = metadata_.get("bands")
        index_name = metadata_.get("index_name")

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
        raw_data["crs"] = crs
        raw_data["crs_json"] = crs_geojson

        if data_folder:
            display_id = scene_.get("display_id")
            with rio.open(
                f"{data_folder_}/{display_id}__{index_name}.TIF", "w", **extra
            ) as src:
                src.write(index_result, 1)

            json.dump(
                fc(vector_data, crs=crs_geojson),
                open(f"{data_folder_}/{display_id}__{index_name}_VECTOR.geojson", "w"),
                indent=1,
            )

        return scene_

    scenes_index = Parallel(n_jobs=-1)(
        delayed(calculate_index)(scene, metadata, data_folder)
        for scene in tqdm(scenes, desc="calculate  MNDWI, vector")
    )
    return scenes_index
