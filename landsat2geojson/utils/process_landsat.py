from tqdm import tqdm
from joblib import Parallel, delayed
import logging
import numpy as np

logger = logging.getLogger("__name__")


def calculate_mndwi_feature(scenes):
    def calculate_mndwi(scene_):
        raw_data = scene_.get("raw_data")
        band3 = raw_data.get("B3").get("data_read").get("out_image").read(1)
        band6 = raw_data.get("B6").get("data_read").get("out_image").read(1)
        # band3 = band3_file.read(1).astype("float16")
        # band6 = band6_file.read(1).astype("float16")

        mndwi = np.where((band3 + band6) == 0.0, 0, (band3 - band6) / (band3 + band6))
        scene_["raw_data"]["MNDWI"] = mndwi
        return scene_

    scenes_mndwi = Parallel(n_jobs=-1)(
        delayed(calculate_mndwi)(scene)
        for scene in tqdm(scenes, desc="calculate  MNDWI")
    )
    return scenes_mndwi
