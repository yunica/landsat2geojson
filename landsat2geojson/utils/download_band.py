import os
import requests
from joblib import Parallel, delayed
import logging
from .constants import PATH_SW_LANDSAT, TIMEOUT
from .feature_utils import create_folder
from tqdm import tqdm

logger = logging.getLogger('__name__')


def download_scene(feature_, bands_, data_folder_):
    props = feature_.get("properties", {})
    id = f"{feature_.get('id', '').split('_02_')[0]}_01_T1".replace("L2SP", "L1TP")
    wrs_path = props.get("landsat:wrs_path", "")
    wrs_row = props.get("landsat:wrs_row", "")

    path_url = f"{PATH_SW_LANDSAT}/{wrs_path}/{wrs_row}/{id}"
    data_folder_band = f'{data_folder_}/{id}'
    create_folder(data_folder_band)
    session = requests.Session()

    def download_tiff(file_url_, file_path_, band_, session_):
        status = {
            'is_download': True,
            'file_url': file_url_,
            'file_path': file_path_,
            'band': band_
        }
        if not os.path.isfile(file_path_):
            r = session_.get(file_url_, timeout=TIMEOUT)
            if r.status_code == 200:
                with open(file_path_, "wb") as f:
                    f.write(r.content)
            else:
                logger.error(f"No found image... {file_url_}")
                status['is_download'] = False
        return status

    status_download = Parallel(n_jobs=-1)(
        delayed(download_tiff)(f"{path_url}/{id}_{band}.TIF", f'{data_folder_band}/{id}_{band}.TIF', band, session)
        for band in bands_
    )
    props['status_download'] = status_download
    return feature_


def download_scenes(scenes, bands, data_folder):
    status_download = [download_scene(feature, bands, data_folder) for feature in
                       tqdm(scenes, desc="download features tiff")]
    return status_download
