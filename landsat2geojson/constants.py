DAYS_AGO = 120
LIMIT_QUERY = 200
FEATURE_CLEAN_FIELDS = [
    "cloud_cover",
    "entity_id",
    "display_id",
    "landsat_product_id",
    "landsat_scene_id",
    "acquisition_date",
    "wrs_path",
    "wrs_row",
    "options",
    "spatial_bounds",
    "spatial_coverage",
]
WATER_BANDS = ["B3", "B6"]
TIMEOUT = 60
QUERY_DATA = {
    "WATER": {
        "bands": ["B3", "B6"],
        "query": 'node["natural"="water"]{bbox};way["natural"="water"]{bbox};',
        "message": "",
        "extra": 'raw_data.get("B3").get("data_read").get("meta")',
        "formula": """np.where(
            ((bands.get('B3') + bands.get('B6')) == 0.0),
             0,
            ((bands.get('B3') - bands.get('B6')) / (bands.get('B3') + bands.get('B6'))))""",
        "index_name": "MNDWI",
    }
}
