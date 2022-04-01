from datetime import datetime, timedelta
from .constants import DAYS_AGO, LIMIT_QUERY

from landsatxplore.api import API

def wraper_landsadxplore(username, password, bbox):
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
