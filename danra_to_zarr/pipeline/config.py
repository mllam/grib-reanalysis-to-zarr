from pathlib import Path

from .. import __version__

# FP_ROOT = Path("/dmidata/projects/cloudphysics/danra/data")
FP_ROOT = Path("/nwp/danra/data")
FP_TEMP_ROOT = Path("/nwp/danra/tempfiles")

VERSION = f"v{__version__.split('+')[0]}"


# The "data collection" may contain multiple named parts (each will be put in its own zarr archive)
# Each part may contain multiple "level types" (e.g. heightAboveGround, etc)
# and a name-mapping may also be defined


DATA_COLLECTION = dict(
    description="All prognostic variables for 10-year period on reduced levels",
    rechunk_to=dict(time=128, x=128, y=128),
    intermediate_time_partitioning=["P14D", "P26W"],
    timespan=slice("1990-09-01", "2000-09-01"),
    parts=dict(
        height_levels=dict(
            heightAboveGround=dict(
                variables={
                    # v: [30, 50, 75, 100, 150, 200, 250, 300, 500]
                    v: [100]
                    for v in "t r u v".split()
                }
            )
        ),
        pressure_levels=dict(
            isobaricInhPa=dict(
                variables={
                    # v: [ 100, 200, 250, 300, 400, 500, 600, 700, 800, 850, 900, 925, 950, 1000, ]
                    v: [1000]
                    for v in "z t u v tw r ciwc cwat".split()
                }
            )
        ),
        single_levels=dict(
            heightAboveGround=dict(
                variables={
                    **{
                        v: [0]
                        for v in [
                            "hcc",
                            "icei",
                            "lcc",
                            "lwavr",
                            "mcc",
                            "mld",
                            "pres",
                            "prtp",
                            "psct",
                            "pscw",
                            "pstb",
                            "pstbc",
                            "sf",
                            "swavr",
                            "vis",
                            "xhail",
                            # TODO: `rain`, `snow`, `snsub`, `nlwrs`, `nswrs`, `lhsub`,
                            # `lhe`, `grpl`, `dni`, `grad`, `sshf`, `wevap`, `vflx`, `uflx`, `tp`
                            # use time-range indicator 4, but dmidc currently assumes 0
                            # "rain",
                            # "snow",
                            # "snsub",
                            # "nlwrs",
                            # "nswrs",
                            # "lhsub",
                            # "lhe",
                            # "grpl",
                            # "dni",
                            # "grad",
                            # "sshf",
                            # "wevap",
                            # "vflx",
                            # "uflx",
                            # "tp",
                        ]
                    },
                    **{
                        "t": [0, 2],
                        # TODO: `tmin` and `tmax` use time-range indicator 2, but dmidc currently assumes 0
                        # "tmin": [2],
                        # "tmax": [2],
                        "r": [2],
                        "u": [10],
                        "v": [10],
                        # TODO: in next version add ugst and vgst, but dmidc needs to be updated first
                        # to change timerange_indicator to 10 (rather than 0 by default), need to find
                        # out what value 10 means too..
                        # "ugst": [10],
                        # "vgst": [10],
                    },
                },
                level_name_mapping="{var_name}{level}m",
            ),
            entireAtmosphere=dict(
                variables={v: [0] for v in "pwat cape cb ct grpl".split()},
                level_name_mapping="{var_name}_column",
            ),
            # the variables `nswrt` and `nlwrt` are not available with timeRangeIndicator==0
            # for all source tar-files, and so I am excluding them for now
            # nominalTop=dict(
            #     variables=dict(nswrt=[0], nlwrt=[0]), level_name_mapping="{var_name}_toa"
            # ),
            heightAboveSea=dict(
                variables=dict(pres=[0]), level_name_mapping="{var_name}_seasurface"
            ),
        ),
    ),
)
