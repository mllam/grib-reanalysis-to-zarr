from datetime import datetime
from pathlib import Path


def find_files_for_time_interval(t_start: datetime, t_end: datetime) -> list[str]:
    """
    Return a list of paths to GRIB files for the given time interval.

    Example paths:

    fc2025022600+000grib2_en
    fc2025022600+000grib2_ep
    fc2025022600+000grib2_ie
    fc2025022600+000grib2_ie
    fc2025022600+000grib2_ml
    fc2025022600+000grib2_pp
    fc2025022600+000grib_hy
    fc2025022600+000grib_rt
    fc2025022600+001grib2_en
    fc2025022600+001grib2_ep
    fc2025022600+001grib2_ie
    fc2025022600+001grib2_ie
    fc2025022600+001grib2_ml
    fc2025022600+001grib2_pp
    fc2025022600+001grib_hy
    fc2025022600+001grib_rt
    fc2025022600+002grib2_en
    fc2025022600+002grib2_ep
    fc2025022600+002grib2_ie
    fc2025022600+002grib2_ie
    fc2025022600+002grib2_ml
    fc2025022600+002grib2_pp
    fc2025022600+002grib_hy
    fc2025022600+002grib_rt
    fc2025022600+003grib2_en
    fc2025022600+003grib2_ep


    """

    root_path = Path("/ec/res4/hpcperm/hlam/ml_data/ie/ireps")
    filename_format = "fc{start:%Y%m%d%H}+{hours:03d}{suffix}"
    suffixes = [
        "grib2_en",
        "grib2_ep",
        "grib2_ie",
        "grib2_ml",
        "grib2_pp",
        "grib_hy",
        "grib_rt",
    ]

    paths = []
    t = t_start
    for suffix in suffixes:
        for hours in range(4):
            filename = filename_format.format(start=t, hours=hours, suffix=suffix)
            paths.append(root_path / filename)
    return paths
