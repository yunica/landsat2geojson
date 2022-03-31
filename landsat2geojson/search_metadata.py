import requests
from datetime import datetime, timedelta
from .constants import DAYS_AGO, LIMIT_QUERY

from landsatxplore.api import API


def fetch_sat_api(query):
    """
    Queries the sat-api (STAC) backend.
    This function handles pagination.
    query is a python dictionary to pass as json to the request.
    """
    headers = {
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
        "Accept": "application/geo+json",
    }

    url = "https://landsatlook.usgs.gov/sat-api/stac/search"
    # today = datetime.now().strftime("%Y-%m-%d")
    today = datetime(2021, 7, 1)
    months_ago = (today - timedelta(days=DAYS_AGO)).strftime("%Y-%m-%d")
    query.update(
        {
            "query": {
                "collections": ["landsat-c2l2-sr", "landsat-c2l2-st"],
                "eo:cloud_cover": {"lte": 0.5},
                "platform": {"in": ["LANDSAT_8"]},
                "landsat:collection_category": {"in": ["T1", "T2",]},
            },
            "limit": LIMIT_QUERY,
            "time": f'{months_ago}/{today.strftime("%Y-%m-%d")}',
        }
    )
    data = requests.post(url, headers=headers, json=query).json()
    error = data.get("message", "")
    if error:
        raise Exception(f"SAT-API failed and returned: {error}")

    meta = data.get("meta", {})
    if not meta.get("found"):
        return {}
    return data


def wraper_landsadxplore(username, password, bbox):
    api = API(username, password)
    today = datetime.today()
    end = (today - timedelta(days=DAYS_AGO)).strftime("%Y-%m-%d")
    where = {
        "dataset": "landsat_ot_c2_l2",
        "bbox": bbox,
        "start_date": end,
        "end_date": today.strftime("%Y-%m-%d"),
        "max_results": 200,
    }
    results = api.search(**where)
    api.logout()
    return results


# search --dataset landsat_ot_c2_l2 --location 12.53 -1.53 --start 2022-01-01 --end 2022-12-31
