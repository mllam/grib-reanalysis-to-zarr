from datetime import datetime

def find_files_for_time_interval(start: datetime, end: datetime) -> list[str]:
    return [
        f"/some/path/to/gribs/{start:%Y%m%d%H}.grib",
        f"/some/path/to/gribs/{end:%Y%m%d%H}.grib"
    ]