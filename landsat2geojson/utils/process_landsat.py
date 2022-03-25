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


def calculate_mndwi_feature(scenes, data_folder):
    def calculate_mndwi(scene_, data_folder_):
        raw_data = scene_.get("raw_data")
        band3 = raw_data.get("B3").get("data_read").get("out_image")
        band6 = raw_data.get("B6").get("data_read").get("out_image")
        extra = raw_data.get("B3").get("data_read").get("meta")

        mndwi = np.where(
            ((band3 + band6) == 0.0), 0, ((band3 - band6) / (band3 + band6))
        )
        raw_data["MNDWI"] = mndwi

        # vector
        transform_ = extra.get("transform")
        crs = get_crs_dataset(extra.get("crs"))
        crs_geojson = {
            "type": "name",
            "properties": {"name": f"urn:ogc:def:crs:EPSG::{crs}"},
        }

        mndwi_32 = mndwi.astype("float32")
        mask = mndwi != 0
        vector_data = []
        for (p, v) in rio_shape(np.round(mndwi_32, 0), mask=mask, transform=transform_):
            vector_data.append(
                {"type": "Feature", "properties": {"val": v}, "geometry": p}
            )
        raw_data["MNDWI_vector"] = vector_data
        raw_data["crs"] = crs
        raw_data["crs_json"] = crs_geojson

        if data_folder:
            display_id = scene_.get("display_id")
            with rio.open(
                f"{data_folder_}/{display_id}__MNDWI.TIF", "w", **extra
            ) as src:
                src.write(mndwi, 1)

            json.dump(
                fc(vector_data, crs=crs_geojson),
                open(f"{data_folder_}/{display_id}__MNDWI_VECTOR.geojson", "w"),
                indent=1,
            )

        return scene_

    scenes_mndwi = Parallel(n_jobs=-1)(
        delayed(calculate_mndwi)(scene, data_folder)
        for scene in tqdm(scenes, desc="calculate  MNDWI, vector")
    )
    return scenes_mndwi
