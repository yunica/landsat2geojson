import os
from shapely.geometry import mapping, shape, box, Polygon
from shapely.ops import unary_union
from tqdm import tqdm
from joblib import Parallel, delayed
import geopandas as gpd
import rasterio as rio
from rasterio import mask
import logging

logger = logging.getLogger('__name__')


def clip_features(scenes):
    def clip_raster(status_):
        try:
            raster_path = status_.get('file_path')
            features_contains = status_.get('feature').get('features_contains')
            gds = gpd.GeoSeries([i['geom'] for i in features_contains]).set_crs(4326)
            with rio.open(raster_path) as src:
                if '4326' not in str(src.crs):
                    gds = gds.to_crs(int(str(src.crs).split(':')[1]))

                out_image, out_transform = mask.mask(src, list(gds), crop=True)
                out_meta = src.meta
                out_meta.update({"driver": "GTiff",
                                 "nodata": 0,
                                 "height": out_image.shape[1],
                                 "width": out_image.shape[2],
                                 "transform": out_transform})
                raster_path_out = raster_path.replace('.TIF', '__CLIP.TIF')
                with rio.open(raster_path_out, "w", **out_meta) as dest:
                    dest.write(out_image)
        except Exception as ex:
            logger.error(ex.__str__())

    compile_files = [{'file_path': status.get('file_path', ''), 'feature': feature} for feature in scenes for
                     status in feature.get('properties', {}).get('status_download', [])]
    Parallel(n_jobs=-1)(
        delayed(clip_raster)(status)
        for status in tqdm(compile_files, desc="clip  tiff")
    )
