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
# t_end = "1990-09-03T00:00Z"

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
    interval: FP_DIMIDATA_ROOT / "tempfiles" for interval in TIME_PARTITIONS_ON_DMIDATA
}
FP_TEMP_ROOT["complete"] = FP_DIMIDATA_ROOT / "tempfiles"

SHOULD_DELETE = TIME_PARTITIONS_LOCALLY + TIME_PARTITIONS_ON_DMIDATA[:-1]

DELETE_INTERMEDIATE_ZARR_FILES = {
    dt_str: dt_str in SHOULD_DELETE for dt_str in INTERMEDIATE_TIME_PARTITIONING
}
DELETE_INTERMEDIATE_ZARR_FILES["collection-with-attrs"] = True
DELETE_INTERMEDIATE_ZARR_FILES["complete"] = False
DELETE_INTERMEDIATE_ZARR_FILES["CONSTANTS"] = True


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
        static_fields=dict(),
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
        single_levels=dict(
            heightAboveGround=dict(
                variables={
                    **{
                        v: [0]
                        for v in [
                            "hcc",
                            "lcc",
                            "mcc",
                            "tcc",
                            "icei",
                            "lwavr",
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
                level_name_mapping="{var_name}{level:d}m",
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
            # This level-type named "CONSTANTS" is used to indicate we want
            # constant fields (land-sea mask and surface geopotential)
            CONSTANTS=dict(variables=dict(lsm=[0], z=[0])),
        ),
    ),
)
# https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html

CF_CANONICAL_UNITS = dict(
    air_temperature="K",
    relative_humidity="%",  # XXX: should be `1`
    x_wind="m s-1",
    y_wind="m s-1",
    geopotential="m2 s-2",
    atmosphere_mass_content_of_cloud_ice="kg m-2",
    atmosphere_mass_content_of_cloud_liquid_water="kg m-2",
    low_type_cloud_area_fraction="1",
    medium_type_cloud_area_fraction="1",
    high_type_cloud_area_fraction="1",
    cloud_area_fraction="1",
    atmosphere_convective_available_potential_energy_wrt_surface="J kg-1",
    cloud_base_altitude="m",
    cloud_top_altitude="m",
    atmosphere_mass_content_of_graupel="kg m-2",
    brightness_temperature_at_cloud_top="K",
    snowfall_amount="kg m-2",
    visibility_in_air="m",
    air_pressure="Pa",
    upward_air_velocity="m s-1",
    atmosphere_mass_content_of_water_vapor="kg m-2",
    air_pressure_at_mean_sea_level="Pa",
    surface_geopotential="m**2 s**-2",
    land_binary_mask="1",
    atmosphere_boundary_layer_thickness="m",
    predominant_precipitation_type_at_surface=None,  # doens't have any canonical units
)

CF_EXTRA_FIELDS = dict(
    predominant_precipitation_type_at_surface=dict(
        flag_values=[],
        flag_meanings=[],
    )
)

CF_STANDARD_NAME_TO_DANRA_NAME = dict(
    CONSTANTS=dict(orog="surface_geopotential", lsm="land_binary_mask"),
    isobaricInhPa=dict(
        z="geopotential",
        t="air_temperature",
        u="x_wind",
        v="y_wind",
        tw="upward_air_velocity",
        r="relative_humidity",
        ciwc="atmosphere_mass_content_of_cloud_ice",  # NOTE: ciwc is the column integrated cloud ice content
        cwat="atmosphere_mass_content_of_cloud_liquid_water",  # NOTE: cwat is the column integrated cloud liquid water content
    ),
    heightAboveGround=dict(
        t="air_temperature",
        r="relative_humidity",
        u="x_wind",  # NOTE: not u_wind as the winds are grid-aligned and the grid is rotated
        v="y_wind",  # NOTE: same as above
        # single level fields that are given on "heightAboveGround" in DANRA:
        hcc="high_type_cloud_area_fraction",
        mcc="medium_type_cloud_area_fraction",
        lcc="low_type_cloud_area_fraction",
        tcc="cloud_area_fraction",
        icei=None,  # XXX: not sure that what this is
        mld="atmosphere_boundary_layer_thickness",
        pres="air_pressure",
        lwavr=None,  # XXX: not sure whether this near-surface or top-of-atmosphere longwave radiation flux
        swavr=None,  # XXX: not sure whether this is near-surface of top-of-atmosphere shortwave radiation flux
        prtp="predominant_precipitation_type_at_surface",
        psct="brightness_temperature_at_cloud_top",  # XXX: is missing units in GRIB files
        pscw=None,  # doesn't appear to have an equivalent in CF conventions
        pstb=None,  # doesn't appear to have an equivalent in CF conventions
        pstbc=None,  # doesn't appear to have an equivalent in CF conventions
        sf="snowfall_amount",
        vis="visibility_in_air",
        xhail=None,  # XXX: I think this might be "hail_fall_amount" but I'm not sure about the accumulation window
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
    ),
    entireAtmosphere=dict(
        # CAPE appears be calculated wrt surface in Harmonie, https://github.com/Hirlam/Harmonie/blob/abf7cdbe6c4d2460005ab2c00e286198b2f384c1/util/gl/grb/cape.f90#L35
        cape="atmosphere_convective_available_potential_energy_wrt_surface",
        cb="cloud_base_altitude",
        ct="cloud_top_altitude",
        grpl="atmosphere_mass_content_of_graupel",
        pwat="atmosphere_mass_content_of_water_vapor",
    ),
    heightAboveSea=dict(pres="air_pressure_at_mean_sea_level"),
)


VARIABLE_RENAMING = dict(
    # These are all fields which shouldn't have been on "height levels" in my
    # (Leif) opinion. Rather than implementing functiontionality to make the
    # formatting string conditional I will just rename these variables before
    # writing the final output
    heightAboveGround=dict(
        hcc0m="hcc",
        icei0m="icei",
        lcc0m="lcc",
        lwavr0m="lwavr",
        mcc0m="mcc",
        mld0m="mld",
        prtp0m="prtp",
        psct0m="psct",
        pscw0m="pscw",
        pstb0m="pstb",
        pstbc0m="pstbc",
        swavr0m="swavr",
        xhail0m="xhail",
    )
)
