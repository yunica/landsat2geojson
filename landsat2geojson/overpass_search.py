import json
import logging

import overpass
from geojson.feature import FeatureCollection as fc

from .feature_utils import line2polygon

logger = logging.getLogger("__name__")


def get_overpass_data(bbox, query, data_folder):
    """
    The get_overpass_data function get a result of the query in overpass.

    Args:
        bbox (tuple): A tuple os coords for bbox.
        query (str): A query from metadata.
        data_folder (str): path of directory.

    Returns:
         dict : A featureCollection object.
    """
    try:
        api = overpass.API(timeout=600)

        query = query.format(bbox=str(bbox))
        response = api.get(query, verbosity="geom")
        response = fc(line2polygon(response.get("features")))
        if response and data_folder:
            json.dump(response, open(f"{data_folder}/overpass_VECTOR.geojson", "w"))
        return response
    except Exception as ex:
        logger.error(ex)
