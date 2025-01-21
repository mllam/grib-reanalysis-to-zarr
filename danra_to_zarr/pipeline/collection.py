import datetime
import io
import shutil

import cfunits.units
import isodate
import luigi
import numpy as np
import pandas as pd
import parse
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata

from ..utils.print_versions import show_versions
from . import config
from .base import (
    DanraZarrSubset,
    DanraZarrSubsetAggregated,
    ZarrTarget,
    should_delete_intermediate,
)
from .utils import convert_markdown_to_html

DANRA_CRS_WKT = """
PROJCRS["DMI HARMONIE DANRA lambert projection",
    BASEGEOGCRS["DMI HARMONIE DANRA lambert CRS",
        DATUM["DMI HARMONIE DANRA lambert datum",
            ELLIPSOID["Sphere", 6367470, 0,
                LENGTHUNIT["metre", 1, ID["EPSG", 9001]]
            ]
        ],
        PRIMEM["Greenwich", 0,
            ANGLEUNIT["degree", 0.0174532925199433, ID["EPSG", 8901]]
        ],
        ID["EPSG",4035]
    ],
    CONVERSION["Lambert Conic Conformal (2SP)",
        METHOD["Lambert Conic Conformal (2SP)", ID["EPSG", 9802]],
        PARAMETER["Latitude of false origin", 56.7,
            ANGLEUNIT["degree", 0.0174532925199433, ID["EPSG", 8821]]
        ],
        PARAMETER["Longitude of false origin", 25,
            ANGLEUNIT["degree", 0.0174532925199433, ID["EPSG", 8822]]
        ],
        PARAMETER["Latitude of 1st standard parallel", 56.7,
            ANGLEUNIT["degree", 0.0174532925199433, ID["EPSG", 8823]]
        ],
        PARAMETER["Latitude of 2nd standard parallel", 56.7,
            ANGLEUNIT["degree", 0.0174532925199433, ID["EPSG", 8824]]
        ],
        PARAMETER["Easting at false origin", 0,
            LENGTHUNIT["metre", 1, ID["EPSG", 8826]]
        ],
        PARAMETER["Northing at false origin", 0,
            LENGTHUNIT["metre", 1, ID["EPSG", 8827]]
        ]
    ],
    CS[Cartesian, 2],
    AXIS["(E)", east, ORDER[1],
        LENGTHUNIT["metre", 1, ID["EPSG", 9001]]
    ],
    AXIS["(N)", north, ORDER[2],
        LENGTHUNIT["metre", 1, ID["EPSG", 9001]]
    ],
    USAGE[
        AREA["Denmark and surrounding regions"],
        BBOX[47, -3, 65, 25],
        SCOPE["Danra reanalysis projection"]
    ]
]
"""


DANRA_CRS_ATTRS = {
    "crs_wkt": "".join(DANRA_CRS_WKT.splitlines()),
    "semi_major_axis": 6367470.0,
    "semi_minor_axis": 6367470.0,
    "inverse_flattening": 0.0,
    "reference_ellipsoid_name": "Sphere",
    "longitude_of_prime_meridian": 0.0,
    "prime_meridian_name": "Greenwich",
    "geographic_crs_name": "DMI HARMONIE DANRA lambert CRS",
    "horizontal_datum_name": "DMI HARMONIE DANRA lambert datum",
    "projected_crs_name": "DMI HARMONIE DANRA lambert projection",
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [0.0567, 0.0567],
    "latitude_of_projection_origin": 0.0567,
    "longitude_of_central_meridian": 0.025,
    "false_easting": 0.0,
    "false_northing": 0.0,
}


def _add_projection_info_to_all_variables(ds):
    """
    Add CF-compliant (http://cfconventions.org/cf-conventions/cf-conventions.html#appendix-grid-mappings)
    projection info by adding a new variable that holds the projection attributes and setting on each variable
    that this projection applies.

    NOTE: currently gribscan doesn't return the projection information attributes when parsing
    Harmonie GRIB files. For that reason the projection parameters are hardcoded here. They
    should be moved in future so that this information is returned by gribscan and set in dmidc

    Parameters
    ----------
    ds: xr.Dataset
        the dataset to add projection info to

    Returns
    -------
    Nothing
    """
    PROJECTION_IDENTIFIER = "danra_projection"
    ds[PROJECTION_IDENTIFIER] = xr.DataArray()
    ds[PROJECTION_IDENTIFIER].attrs.update(DANRA_CRS_ATTRS)

    for var_name in ds.data_vars:
        ds[var_name].attrs["grid_mapping"] = PROJECTION_IDENTIFIER


def _units_equal(u1, u2):
    return cfunits.units.Units(u1).equals(cfunits.units.Units(u2))


def _add_standard_names_and_check_units(ds, level_type, level_name_mapping):
    for var_name in list(ds.data_vars):
        if level_name_mapping is not None:
            # XXX: this is a hack, but because I've split individual levels
            # into separate variables I need to extract the variable name from
            # that new name
            name_parts = parse.parse(level_name_mapping, var_name)
            orig_var_name = name_parts.named["var_name"]
        else:
            orig_var_name = var_name

        standard_name = config.CF_STANDARD_NAME_TO_DANRA_NAME[level_type].get(
            orig_var_name
        )
        if standard_name is None:
            logger.warning(f"No standard name found for {var_name}")
            continue

        expected_units = config.CF_CANONICAL_UNITS[standard_name]
        if expected_units is None:
            pass
        else:
            ds[var_name].attrs["standard_name"] = standard_name
            units = ds[var_name].units

            # the land-sea mask units are "(0 - 1)" by default, this isn't a valid unit...
            if units == "(0 - 1)":
                units = "1"
                ds[var_name].attrs["units"] = units

            if (
                standard_name == "brightness_temperature_at_cloud_top"
                and orig_var_name == "psct"
                and units == "-"
            ):
                # the units are actually Kelvin I think, judging from the values
                units = "K"
                ds[var_name].attrs["units"] = units

            if not _units_equal(units, expected_units):
                raise Exception(
                    f"Units missmatch for {var_name}, {standard_name}"
                    f" {units} != {expected_units} "
                )

    # XXX: the units also need to be set on the x and y coordinates, this
    # should be done by dmidc
    assert "units" not in ds.x.attrs
    assert "units" not in ds.y.attrs
    ds["x"].attrs["units"] = "m"
    ds["y"].attrs["units"] = "m"

    ds.x.attrs["standard_name"] = "projection_x_coordinate"
    ds.y.attrs["standard_name"] = "projection_y_coordinate"
    ds.x.attrs["axis"] = "X"
    ds.y.attrs["axis"] = "Y"

    if "time" in ds.coords:
        # this is optional for time, but lets do it to be complete
        ds.time.attrs["standard_name"] = "time"

    if "pressure" in ds.coords:
        ds.pressure.attrs["standard_name"] = "air_pressure"
        ds.pressure.attrs["axis"] = "Z"

    if "altitude" in ds.coords:
        ds.altitude.attrs["standard_name"] = "altitude"
        ds.altitude.attrs["positive"] = "up"
        ds.altitude.attrs["axis"] = "Z"


def _set_dim_order_for_cf_compliance(ds):
    for var_name in list(ds.data_vars):
        dims = list(ds[var_name].dims)
        has_time = False
        if "time" in dims:
            has_time = True
            dims.remove("time")

        has_xy = False
        if "y" in dims and "x" in dims:
            has_xy = True
            dims.remove("x")
            dims.remove("y")

        assert len(dims) < 2

        if has_xy:
            dims = dims + ["y", "x"]
        if has_time:
            dims = ["time"] + dims

        ds[var_name] = ds[var_name].transpose(*dims)

        # need to remove chunking info when we transpose
        if "chunks" in ds[var_name].encoding:
            ds[var_name].encoding.pop("chunks")


class DanraZarrCollection(luigi.Task):
    """
    Create one part of DANRA dataset using the mappings from
    level-type, variable name and levels to collection parts defined in config.DATA_COLLECTION
    """

    part_id = luigi.Parameter()

    def requires(self):
        collection_details = config.DATA_COLLECTION
        timespan = collection_details["timespan"]

        tasks = {}
        part_id = self.part_id
        part_contents = collection_details["parts"][part_id]

        for level_type, level_type_variables in part_contents.items():
            tasks[level_type] = {}

            if level_type == "CONSTANTS":
                # for constants the time is irrelevant to the data but the
                # luigi tasks require them, so we just use the same start and
                # end time
                task = DanraZarrSubset(
                    t_start=isodate.parse_date(timespan.start),
                    t_end=isodate.parse_date(timespan.start)
                    + datetime.timedelta(hours=3),
                    aggregate_name="CONSTANTS",
                    variables=level_type_variables["variables"],
                    level_type="CONSTANTS",
                    rechunk_to=collection_details["rechunk_to"],
                )
            else:
                level_name_mapping = collection_details["parts"][part_id][
                    level_type
                ].get("level_name_mapping")
                task = DanraZarrSubsetAggregated(
                    t_start=isodate.parse_date(timespan.start),
                    t_end=isodate.parse_date(timespan.stop),
                    aggregate_name="complete",
                    t_intervals=config.INTERMEDIATE_TIME_PARTITIONING,
                    variables=level_type_variables["variables"],
                    level_type=level_type,
                    rechunk_to=collection_details["rechunk_to"],
                    level_name_mapping=level_name_mapping,
                )
            tasks[level_type] = task

        return tasks

    @property
    def aggregate_name(self):
        return "collection-with-attrs"

    def run(self):
        collection_details = config.DATA_COLLECTION
        collection_description = collection_details["description"]

        part_id = self.part_id
        part_inputs = self.input()
        part_config = collection_details["parts"][self.part_id]

        part_contents = {}
        for level_type, level_type_inputs in part_inputs.items():
            ds_level_type = level_type_inputs.open()
            level_name_mapping = part_config[level_type].get("level_name_mapping")
            _add_standard_names_and_check_units(
                ds=ds_level_type,
                level_type=level_type,
                level_name_mapping=level_name_mapping,
            )

            for var_name in ds_level_type.data_vars:
                da_var = ds_level_type[var_name]
                da_var.attrs["level_type"] = level_type
                part_contents[var_name] = da_var

        ds_part = xr.Dataset()
        for var_name, da in part_contents.items():
            ds_part[var_name] = da

        ds_part.attrs["description"] = collection_description

        # set license attribute
        ds_part.attrs[
            "license"
        ] = "CC-BY-4.0: https://creativecommons.org/licenses/by/4.0/"
        ds_part.attrs[
            "contact"
        ] = "Leif Denby <lcd@dmi.dk>, Danish Meteorological Institute"

        ds_part.attrs["Conventions"] = "CF-1.8"
        _add_projection_info_to_all_variables(ds=ds_part)
        # CF conventions require that the time coordinate is first
        _set_dim_order_for_cf_compliance(ds=ds_part)

        # apply renaming defined in config
        vars_to_rename = {
            var_name: new_name
            for (var_name, new_name) in config.VARIABLE_RENAMING.items()
            if var_name in ds_part.data_vars
        }
        ds_part = ds_part.rename(vars_to_rename)

        part_output = self.output()
        part_output.path.parent.mkdir(exist_ok=True, parents=True)
        logger.info(
            f"Writing {config.VERSION} collection part {part_id} to {part_output.path}"
        )
        for var_name in ds_part.data_vars:
            for attr in [
                "NV",
                "gridDefinitionDescription",
                "gridType",
                "missingValue",
                "numberOfPoints",
            ]:
                if attr in ds_part[var_name].attrs:
                    del ds_part[var_name].attrs[attr]

            var_attrs = ds_part[var_name].attrs
            if "name" in var_attrs:
                ds_part[var_name].attrs["long_name"] = ds_part[var_name].attrs.pop(
                    "name"
                )

        part_output.write(ds_part)
        consolidate_metadata(part_output.path)

        if should_delete_intermediate(self.aggregate_name):
            fps_parents = [inp.path for inp in part_inputs.values()]
            logger.info(
                f"Deleting input source files (for {self.aggregate_name}): {fps_parents}"
            )
            for fp_parent in fps_parents:
                shutil.rmtree(fp_parent)

    def output(self):
        # run to check that it is defined for this task whether the child tasks
        # output should be deleted

        should_delete_intermediate(self.aggregate_name)
        path_root = config.FP_ROOT / config.VERSION
        fn = f"{self.part_id}.zarr"
        return ZarrTarget(path_root / fn)


class DanraCompleteZarrCollection(luigi.Task):
    """
    Create full DANRA dataset using the mappings from
    level-type, variable name and levels to collection parts defined in config.DATA_COLLECIONS
    """

    def requires(self):
        collection_details = config.DATA_COLLECTION

        tasks = {}

        for part_id in collection_details["parts"].keys():
            tasks[part_id] = DanraZarrCollection(part_id=part_id)

        return tasks

    def run(self):
        collection_details = config.DATA_COLLECTION
        collection_description = collection_details["description"]

        text_markdown = "# DANRA reanalysis Zarr data collection\n\n"
        text_markdown += f"**{config.VERSION}, created {datetime.datetime.now().replace(microsecond=0).isoformat()}**\n\n"
        text_markdown += f"time-span: {collection_details['timespan'].start} to {collection_details['timespan'].stop}\n\n"
        text_markdown += f"> {collection_description}\n\n"
        inputs = self.input()

        for part_id, part_input in self.input().items():
            ds_part = inputs[part_id].open()
            text_markdown += f"## {part_id.replace('_', ' ')}\n\n"
            text_markdown += f"filename: `{part_input.path.name}`\n\n"
            level_dim = None
            for dim in ["altitude", "pressure"]:
                if dim in ds_part.coords:
                    level_dim = dim

            if level_dim is not None:
                N_levels = len(ds_part[level_dim].values)
                # projection info variable has size==1 so we can remove it from
                # the table based on that size
                var_names = [v for v in list(ds_part.data_vars) if ds_part[v].size != 1]

                N_vars = len(var_names)
                units = ds_part[level_dim].attrs.get("units", "")
                indexes = [f"{v} [{units}]" for v in ds_part[level_dim].values]
                df = pd.DataFrame(
                    columns=var_names,
                    index=indexes,
                    data=np.ones((N_levels, N_vars), dtype=bool),
                )
                df = df.map(lambda v: "✓" if v is True else "")

                cols_new = {}
                for v in df.columns:
                    if "long_name" not in ds_part[v].attrs:
                        raise Exception(ds_part[v])
                    cols_new[v] = f"<abbr title='{ds_part[v].long_name}'>{v}</abbr>"

                df = df.rename(columns=cols_new)

                df = df.T
                df = df[sorted(df.columns)]

                text_markdown += df.to_markdown()
                text_markdown += "\n\n"
            else:
                var_names = list(ds_part.data_vars)
                var_names = sorted(var_names, key=lambda v: ds_part[v].level)
                text_markdown += ", ".join(
                    f"<abbr title='{ds_part[v].long_name}'>{v}</abbr>"
                    for v in var_names
                )

        # include package versions
        fh = io.StringIO()
        show_versions(file=fh)
        text_markdown += "\n\n"
        text_markdown += f"<pre>{fh.getvalue()}</pre>\n\n"

        readme_path = self.output()["README"].path

        with open(readme_path, "w") as f:
            f.write(text_markdown)

        logger.info(f"Wrote README file to {readme_path}")

        convert_markdown_to_html(fp_markdown=readme_path)

    def output(self):
        fn = "README.md"
        fp_root = config.FP_ROOT / config.VERSION

        outputs = dict(self.input())
        outputs["README"] = ZarrTarget(fp_root / fn)
        return outputs
