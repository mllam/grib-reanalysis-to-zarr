from pathlib import Path

import isodate

from .. import __version__

# FP_ROOT = Path("/dmidata/projects/cloudphysics/danra/data")
FP_DIMIDATA_ROOT = Path("/dmidata/projects/cloudphysics/danra/")
# FP_ROOT is the location relative to which the final dataset is written
FP_ROOT = FP_DIMIDATA_ROOT / "data"
# FP_TEMP_ROOT_DEFAULT is the path relative to which temporary files
# (zarr-dataset and extracted GRIB files) are written. This should generally be
# on local fast storage. Where this path isn't large enough this temporary path
# can be overridden for different time aggregated dataset by setting
# FP_TEMP_ROOT for different aggregate ranges below
FP_TEMP_ROOT_DEFAULT = Path("/vf/danra/tempfiles")

VERSION = f"v{__version__.split('+')[0]}"

t_start = "1990-09-01T00:00Z"
t_end = "2024-01-01T00:00Z"

# 32 days with 3 samples/day => 256 samples, which matches chunksize 256 in time
# once we have a full chunk in time we store this on dmidata (since this will
# reduce the number of small files)
t_chunk_size = 256
dt_timestep = isodate.parse_duration("PT3H")
dt_chunk = t_chunk_size * dt_timestep

TIME_PARTITIONS_LOCALLY = ["P1D"]
TIME_PARTITIONS_ON_DMIDATA = [
    isodate.duration_isoformat(dt) for dt in [dt_chunk, dt_chunk * 6]
]
INTERMEDIATE_TIME_PARTITIONING = TIME_PARTITIONS_LOCALLY + TIME_PARTITIONS_ON_DMIDATA

# root path for temporary data that shouldn't be stored in the default temp path
FP_TEMP_ROOT = {
    isodate.parse_duration(interval): FP_DIMIDATA_ROOT / "tempfiles"
    for interval in TIME_PARTITIONS_ON_DMIDATA
}

SHOULD_DELETE = TIME_PARTITIONS_LOCALLY + TIME_PARTITIONS_ON_DMIDATA[:-1]

DELETE_INTERMEDIATE_ZARR_FILES = {
    dt_str: dt_str in SHOULD_DELETE for dt_str in INTERMEDIATE_TIME_PARTITIONING
}
DELETE_INTERMEDIATE_ZARR_FILES["collection-with-attrs"] = True
DELETE_INTERMEDIATE_ZARR_FILES["complete"] = False


# The "data collection" may contain multiple named parts (each will be put in its own zarr archive)
# Each part may contain multiple "level types" (e.g. heightAboveGround, etc)
# and a name-mapping may also be defined

DATA_COLLECTION = dict(
    description=f"All prognostic variables for {t_start} to {t_end} on all levels",
    # x and y chunksize are created so that domain is split into 3x2=6 roughly
    # equal chunks that we time chunksize of 256 gives ~100MB chunks for each
    # level/single-level field
    rechunk_to=dict(time=256, x=263, y=295, pressure=1, altitude=1),
    timespan=slice(t_start, t_end),
    parts=dict(
        height_levels=dict(
            heightAboveGround=dict(
                variables={
                    v: [30, 50, 75, 100, 150, 200, 250, 300, 500]
                    for v in "t r u v".split()
                }
            )
        ),
        pressure_levels=dict(
            isobaricInhPa=dict(
                variables={
                    v: [
                        1000,
                        950,
                        925,
                        900,
                        850,
                        800,
                        700,
                        600,
                        500,
                        400,
                        300,
                        250,
                        200,
                        100,
                    ]
                    for v in "z t u v tw r ciwc cwat".split()
                }
            )
        ),
        # single_levels=dict(
        #     heightAboveGround=dict(
        #         variables={
        #             **{
        #                 v: [0]
        #                 for v in [
        #                     "hcc",
        #                     "icei",
        #                     "lcc",
        #                     "lwavr",
        #                     "mcc",
        #                     "mld",
        #                     "pres",
        #                     "prtp",
        #                     "psct",
        #                     "pscw",
        #                     "pstb",
        #                     "pstbc",
        #                     "sf",
        #                     "swavr",
        #                     "vis",
        #                     "xhail",
        #                     # TODO: `rain`, `snow`, `snsub`, `nlwrs`, `nswrs`, `lhsub`,
        #                     # `lhe`, `grpl`, `dni`, `grad`, `sshf`, `wevap`, `vflx`, `uflx`, `tp`
        #                     # use time-range indicator 4, but dmidc currently assumes 0
        #                     # "rain",
        #                     # "snow",
        #                     # "snsub",
        #                     # "nlwrs",
        #                     # "nswrs",
        #                     # "lhsub",
        #                     # "lhe",
        #                     # "grpl",
        #                     # "dni",
        #                     # "grad",
        #                     # "sshf",
        #                     # "wevap",
        #                     # "vflx",
        #                     # "uflx",
        #                     # "tp",
        #                 ]
        #             },
        #             **{
        #                 "t": [0, 2],
        #                 # TODO: `tmin` and `tmax` use time-range indicator 2, but dmidc currently assumes 0
        #                 # "tmin": [2],
        #                 # "tmax": [2],
        #                 "r": [2],
        #                 "u": [10],
        #                 "v": [10],
        #                 # TODO: in next version add ugst and vgst, but dmidc needs to be updated first
        #                 # to change timerange_indicator to 10 (rather than 0 by default), need to find
        #                 # out what value 10 means too..
        #                 # "ugst": [10],
        #                 # "vgst": [10],
        #             },
        #         },
        #         level_name_mapping="{var_name}{level}m",
        #     ),
        #     entireAtmosphere=dict(
        #         variables={v: [0] for v in "pwat cape cb ct grpl".split()},
        #         level_name_mapping="{var_name}_column",
        #     ),
        #     # the variables `nswrt` and `nlwrt` are not available with timeRangeIndicator==0
        #     # for all source tar-files, and so I am excluding them for now
        #     # nominalTop=dict(
        #     #     variables=dict(nswrt=[0], nlwrt=[0]), level_name_mapping="{var_name}_toa"
        #     # ),
        #     heightAboveSea=dict(
        #         variables=dict(pres=[0]), level_name_mapping="{var_name}_seasurface"
        #     ),
        # ),
    ),
)
