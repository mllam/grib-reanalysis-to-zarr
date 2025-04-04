from datetime import datetime
from pathlib import Path

def find_files_for_time_interval(t_start: datetime, t_end: datetime) -> list[str]:

    root_path = Path("/scratch/duuw/reanalysis/mera/")

    # Example: MERA_PRODYEAR_2017_12_11_105_2_0_ANALYSIS
    filename_format = "MERA_PRODYEAR_{start:%Y_%m_%d%H}_{leveltype}_{level}_{timerangeindicator}_{suffix}"
    suffixes = ["ANALYSIS"]
    
    leveltype = "105"
    level = "2"
    timerangeindicator = "0"

    paths = []
    t = t_start
    for suffix in suffixes:
        for hours in range(4):
            filename = filename_format.format(start=t, 
                                              leveltype=leveltype,
                                              timerangeindicator=timerangeindicator,
                                              suffix=suffix)
            paths.append(root_path / filename)

    return paths