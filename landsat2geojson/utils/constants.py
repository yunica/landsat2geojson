DAYS_AGO = 120
LIMIT_QUERY = 500
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
PATH_SW_LANDSAT = "https://landsat-pds.s3.amazonaws.com/c1/L8"
WATER_BANDS = ["B3", "B6"]
TIMEOUT = 60
