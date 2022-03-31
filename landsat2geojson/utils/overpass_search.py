import overpass
import logging
import json
from .feature_utils import line2polygon
from geojson.feature import FeatureCollection as fc

logger = logging.getLogger("__name__")


def get_overpass_data(bbox, query, data_folder):
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
