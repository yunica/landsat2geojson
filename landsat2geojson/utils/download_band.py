import io
import os
import requests
from joblib import Parallel, delayed
import logging
from tqdm import tqdm
from landsatxplore.earthexplorer import EarthExplorer
from .constants import WATER_BANDS
from landsatxplore.errors import EarthExplorerError
import rasterio as rio
from rasterio import mask
import copy
import geopandas as gpd
from .feature_utils import create_folder, clean_path, url2name, get_crs_dataset

logger = logging.getLogger("__name__")

EE_DOWNLOAD_URL = (
    "https://earthexplorer.usgs.gov/download/{data_product_id}/{display_id}"
)


class WrapperEarthExplorer(EarthExplorer):
    def _read_from_path(self, file_path, features_contains):
        dataset = rio.open(file_path,)
        if "4326" not in str(dataset.crs) and str(dataset.crs):
            features_contains = features_contains.to_crs(get_crs_dataset(dataset))

        out_image, out_transform = mask.mask(
            dataset, list(features_contains), crop=True
        )
        out_meta = dataset.meta
        out_meta.update(
            {
                "nodata": 0,
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
            }
        )
        return {
            "profile": copy.deepcopy(dataset.profile),
            "meta": out_meta,
            "out_image": out_image,
        }

    def _save_file_path(self, byio, output_dir, filename_):
        if output_dir:
            with open(f"{output_dir}/{filename_}.TIF", "wb") as src:
                src.write(byio.getvalue())

    def _download(
        self, url, features_contains, output_dir, timeout, chunk_size=1024, skip=False
    ):
        """Download remote file given its URL."""
        # Check availability of the requested product
        # EarthExplorer should respond with JSON

        if os.path.exists(f"{output_dir}/{url2name(url)}.TIF"):
            return self._read_from_path(
                f"{output_dir}/{url2name(url)}.TIF", features_contains
            )

        with self.session.get(
            url, allow_redirects=False, stream=True, timeout=timeout
        ) as r:
            r.raise_for_status()
            error_msg = r.json().get("errorMessage")
            if error_msg:
                raise EarthExplorerError(error_msg)
            download_url = r.json().get("url")
        try:

            with self.session.get(
                download_url, stream=True, allow_redirects=True, timeout=timeout
            ) as r:
                file_size = int(r.headers.get("Content-Length"))
                with tqdm(
                    desc=f"download  {url2name(url)} file",
                    total=file_size,
                    unit_scale=True,
                    unit="B",
                    unit_divisor=1024,
                ) as pbar:

                    byio = io.BytesIO()
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            byio.write(chunk)
                            pbar.update(chunk_size)
                    self._save_file_path(byio, output_dir, f"{url2name(url)}")
                    # check save file or memory
                    with rio.io.MemoryFile(byio.getvalue()) as memfile:
                        dataset = memfile.open()
                        if "4326" not in str(dataset.crs) and str(dataset.crs):
                            features_contains = features_contains.to_crs(
                                get_crs_dataset(dataset)
                            )

                        out_image, out_transform = mask.mask(
                            dataset, list(features_contains), crop=True
                        )
                        out_meta = dataset.meta
                        out_meta.update(
                            {
                                "nodata": 0,
                                "height": out_image.shape[1],
                                "width": out_image.shape[2],
                                "transform": out_transform,
                            }
                        )

                        return {
                            "profile": copy.deepcopy(dataset.profile),
                            "meta": out_meta,
                            "out_image": out_image,
                        }

        except requests.exceptions.Timeout:
            raise EarthExplorerError(
                "Connection timeout after {} seconds.".format(timeout)
            )

    def download(
        self,
        display_id,
        features_contains,
        output_dir="",
        dataset=None,
        timeout=300,
        skip=False,
    ):
        create_folder(output_dir)
        urls = [
            EE_DOWNLOAD_URL.format(
                data_product_id="5f85f0419985f2aa",
                display_id=f"L2SR_{display_id}_SR_{i}_TIF",
            )
            for i in WATER_BANDS
        ]
        data_out = {}
        for url, band in zip(urls, WATER_BANDS):
            data_read = self._download(
                url, features_contains, output_dir, timeout=timeout, skip=skip
            )
            data_out[band] = {
                "name": url.split("/")[-1],
                "data_read": data_read,
            }
        return data_out


def download_scenes(username, password, scenes, output_dir):
    output_dir = clean_path(output_dir)
    ee = WrapperEarthExplorer(username, password)

    def download_scene(ee_, scene_, output_dir_):
        display_id = scene_.get("display_id")
        features_contains = gpd.GeoSeries(
            [i["geom"] for i in scene_.get("features_contains")]
        ).set_crs(4326)
        if not ee_.logged_in():
            ee_ = WrapperEarthExplorer(username, password)
        scene_["raw_data"] = ee_.download(display_id, features_contains, output_dir_)
        return scene_

    scenes_download = Parallel(n_jobs=-1)(
        delayed(download_scene)(ee, scene, output_dir)
        for scene in tqdm(scenes, desc="Prepare download data")
    )
    return scenes_download
