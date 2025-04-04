from datetime import datetime
from pathlib import Path

def find_files_for_time_interval(t_start: datetime, t_end: datetime) -> list[str]:

    root_path = Path("/scratch/duuw/reanalysis/mera/")

    # Example: MERA_PRODYEAR_2017_12_11_105_2_0_ANALYSIS
    path_format = "{indicatorofparameter}/{leveltype}/{level}/{timerangeindicator}"
    filename_format = "MERA_PRODYEAR_{start:%Y_%m}_{indicatorofparameter}_{leveltype}_{level}_{timerangeindicator}_{suffix}"
    suffixes = ["ANALYSIS"]
    
    indicatorofparameter = "11"
    leveltype = "105"
    level = "2"
    timerangeindicator = "0"

    paths = []
    t = t_start
    for suffix in suffixes:

        path_file = path_format.format(indicatorofparameter=indicatorofparameter,
                                        leveltype=leveltype,
                                        level=level,
                                        timerangeindicator=timerangeindicator)

        filename = filename_format.format(start=t,
                                          indicatorofparameter=indicatorofparameter,
                                          leveltype=leveltype,
                                          level=level,
                                          timerangeindicator=timerangeindicator,
                                          suffix=suffix)

        paths.append(root_path / path_file / filename)

    print(paths)
    return paths