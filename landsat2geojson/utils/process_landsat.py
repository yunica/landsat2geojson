import os
from shapely.geometry import mapping, shape, box, Polygon
from shapely.ops import unary_union
from tqdm import tqdm
from joblib import Parallel, delayed
import geopandas as gpd
import rasterio as rio
from rasterio import mask
import logging
import numpy as np

logger = logging.getLogger("__name__")


def clip_features(scenes):
    def clip_raster(status_):
        try:
            raster_path = status_.get("file_path")
            features_contains = status_.get("feature").get("features_contains")
            gds = gpd.GeoSeries([i["geom"] for i in features_contains]).set_crs(4326)
            with rio.open(raster_path) as src:
                if "4326" not in str(src.crs):
                    gds = gds.to_crs(int(str(src.crs).split(":")[1]))

                out_image, out_transform = mask.mask(src, list(gds), crop=True)
                out_meta = src.meta
                out_meta.update(
                    {
                        "driver": "GTiff",
                        "nodata": 0,
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform,
                    }
                )
                raster_path_out = raster_path.replace(".TIF", "__CLIP.TIF")
                with rio.open(raster_path_out, "w", **out_meta) as dest:
                    dest.write(out_image)
        except Exception as ex:
            logger.error(ex.__str__())

    compile_files = [
        {"file_path": status.get("file_path", ""), "feature": feature}
        for feature in scenes
        for status in feature.get("properties", {}).get("status_download", [])
    ]
    Parallel(n_jobs=-1)(
        delayed(clip_raster)(status)
        for status in tqdm(compile_files, desc="clip  tiff")
    )


def calculate_mndwi_feature(scenes):
    def calculate_mndwi(feature_):
        bands = {
            status.get("band"): status
            for status in feature_.get("properties", {}).get("status_download", [])
        }

        band3_file = rio.open(
            bands["B3"].get("file_path").replace(".TIF", "__CLIP.TIF")
        )
        band6_file = rio.open(
            bands["B6"].get("file_path").replace(".TIF", "__CLIP.TIF")
        )

        band3 = band3_file.read(1).astype("float16")
        band6 = band6_file.read(1).astype("float16")

        extra = band6_file.meta.copy()
        extra.update({"nodata": 0, "count": 1, "dtype": "float64"})

        mndwi = np.where((band3 + band6) == 0.0, 0, (band3 - band6) / (band3 + band6))
        mndwi_filter = np.where(mndwi >= 0.02, mndwi, 0)

        file_out_name = bands["B3"].get("file_path").replace("_B3", "_MNDI")
        with rio.open(file_out_name, "w", **extra) as src:
            src.write(mndwi_filter, 1)

    Parallel(n_jobs=-1)(
        delayed(calculate_mndwi)(feature)
        for feature in tqdm(scenes, desc="calculate  mndwi")
    )
