import io
import logging
import os

import geopandas as gpd
import rasterio as rio
import requests
from joblib import Parallel, delayed
from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer
from landsatxplore.errors import EarthExplorerError
from rasterio import mask
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

from .feature_utils import clean_path, create_folder, get_crs_dataset, url2name

logger = logging.getLogger("__name__")

EE_DOWNLOAD_URL = (
    "https://earthexplorer.usgs.gov/download/{data_product_id}/{display_id}"
)


class WrapperEarthExplorer(EarthExplorer):
    def __init__(self, username, password):
        # retry setup
        session_ = requests.Session()

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session_.mount("https://", adapter)
        session_.mount("http://", adapter)

        self.session = session_
        self.login(username, password)
        self.api = API(username, password)

    def _read_from_path(self, file_path, features_contains):
        dataset = rio.open(file_path)
        if "4326" not in str(dataset.crs) and str(dataset.crs):
            features_contains = features_contains.to_crs(get_crs_dataset(dataset.crs))

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
                "count": 1,
                "dtype": "float64",
            }
        )
        return {
            "profile": dataset.profile,
            "meta": out_meta,
            "out_image": out_image.squeeze(),
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
                                    get_crs_dataset(dataset.crs)
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
                                    "count": 1,
                                    "dtype": "float64",
                                }
                            )

                            return {
                                "profile": dataset.profile,
                                "meta": out_meta,
                                "out_image": out_image.squeeze(),
                            }

            except requests.exceptions.Timeout:
                raise EarthExplorerError(
                    "Connection timeout after {} seconds.".format(timeout)
                )

    def download(
        self,
        display_id,
        features_contains,
        bands=[],
        output_dir="",
        dataset=None,
        timeout=600,
        skip=False,
    ):
        create_folder(output_dir)

        urls = [
            EE_DOWNLOAD_URL.format(
                data_product_id="5f85f0419985f2aa",
                display_id=f"L2SR_{display_id}_SR_{i}_TIF",
            )
            for i in bands
        ]
        data_out = {}
        for url, band in zip(urls, bands):
            data_read = self._download(
                url, features_contains, output_dir, timeout=timeout, skip=skip
            )
            data_out[band] = {
                "name": url.split("/")[-1],
                "data_read": data_read,
            }
        return data_out


def download_scenes(
    username: str, password: str, scenes: list, bands: list, output_dir: str
):
    """
    The download_scenes function downloads the bands for each scene.

    Args:
        username (str): username for earthexplorer.
        password (str):  password for earthexplorer.
        scenes (list): A list of scenes with features.
        bands (list): A list of bands.
        output_dir (str): path of directory.

    Returns:
         list : A list of scenes with download bands.
    """

    output_dir = clean_path(output_dir)
    ee = WrapperEarthExplorer(username, password)

    def download_scene_(ee_, scene_, bands_, output_dir_):
        display_id = scene_.get("display_id")
        features_contains = gpd.GeoSeries(
            [i["geom"] for i in scene_.get("features_contains", [])]
        ).set_crs(4326)
        if not ee_.logged_in():
            ee_ = WrapperEarthExplorer(username, password)
        scene_["raw_data"] = ee_.download(
            display_id, features_contains, bands_, output_dir_
        )
        return scene_

    scenes_download = Parallel(n_jobs=-1)(
        delayed(download_scene_)(ee, scene, bands, output_dir)
        for scene in tqdm(scenes, desc="Prepare download data")
    )
    return scenes_download
