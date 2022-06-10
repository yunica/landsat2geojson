from datetime import datetime, timedelta

from landsatxplore.api import API

from .constants import DAYS_AGO, LIMIT_QUERY


def wraper_landsadxplore(username: str, password: str, bbox: tuple):
    """
    The wraper_landsadxplore function get the data to know which landsat scenes are available according to a bbox.

    Args:
        username (str): username for earthexplorer.
        password (str):  password for earthexplorer.
        bbox (tuple): A tuple os coords for bbox.

    Returns:
         dict : The json response.
    """
    api = API(username, password)
    today = datetime.today()
    end = (today - timedelta(days=DAYS_AGO)).strftime("%Y-%m-%d")
    where = {
        "dataset": "landsat_ot_c2_l2",
        "bbox": bbox,
        "start_date": end,
        "end_date": today.strftime("%Y-%m-%d"),
        "max_results": LIMIT_QUERY,
    }
    results = api.search(**where)
    api.logout()
    return results


# search --dataset landsat_ot_c2_l2 --location 12.53 -1.53 --start 2022-01-01 --end 2022-12-31
