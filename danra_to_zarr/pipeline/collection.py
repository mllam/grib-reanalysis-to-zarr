import datetime

import isodate
import luigi
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata

from .. import __version__
from .base import DanraZarrSubsetAggregated, ZarrTarget
from .config import DATA_COLLECTION, FP_ROOT

VERSION = __version__


class DanraZarrCollection(luigi.Task):
    """
    Create one part of DANRA dataset using the mappings from
    level-type, variable name and levels to collection parts defined in DATA_COLLECTION
    """

    part_id = luigi.Parameter()

    def requires(self):
        collection_details = DATA_COLLECTION
        timespan = collection_details["timespan"]

        tasks = {}

        part_id = self.part_id
        part_contents = collection_details["parts"][part_id]

        for level_type, level_type_variables in part_contents.items():
            tasks[level_type] = {}
            for var_name, levels in level_type_variables["variables"].items():
                task = DanraZarrSubsetAggregated(
                    t_start=isodate.parse_date(timespan.start),
                    t_end=isodate.parse_date(timespan.stop),
                    t_interval=isodate.parse_duration("P7D"),
                    variables=[var_name],
                    levels=levels,
                    level_type=level_type,
                    rechunk_to=collection_details["rechunk_to"],
                )
                tasks[level_type][var_name] = task

        return tasks

    def run(self):
        collection_details = DATA_COLLECTION
        collection_description = collection_details["description"]

        part_id = self.part_id
        part_inputs = self.input()

        part_contents = {}
        for level_type, level_type_variables in part_inputs.items():
            name_mapping = collection_details["parts"][part_id][level_type].get(
                "name_mapping"
            )
            for var_name, var_input in level_type_variables.items():
                da_var = var_input.open()[var_name]
                if name_mapping is not None:
                    for level in da_var.level.values:
                        da_var_level = da_var.sel(level=level).drop("level")
                        da_var_level.attrs["level"] = level
                        identifier = name_mapping.format(var_name=var_name, level=level)
                        part_contents[identifier] = da_var_level
                else:
                    identifier = var_name
                    part_contents[var_name] = da_var

        ds_part = xr.Dataset(part_contents)
        ds_part.attrs["description"] = collection_description
        part_output = self.output()
        part_output.path.mkdir(exist_ok=True, parents=True)
        logger.info(
            f"Writing {VERSION} collection part {part_id} to {part_output.path}"
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
            ds_part[var_name].attrs["long_name"] = ds_part[var_name].attrs["name"]
        part_output.write(ds_part)
        consolidate_metadata(part_output.path)

    def output(self):
        path_root = FP_ROOT / "data" / VERSION
        fn = f"{self.part_id}.zarr"
        return ZarrTarget(path_root / fn)


class DanraCompleteZarrCollection(luigi.Task):
    """
    Create full DANRA dataset using the mappings from
    level-type, variable name and levels to collection parts defined in DATA_COLLECIONS
    """

    def requires(self):
        collection_details = DATA_COLLECTION

        tasks = {}

        for part_id in collection_details["parts"].keys():
            tasks[part_id] = DanraZarrCollection(part_id=part_id)

        return tasks

    def run(self):
        collection_details = DATA_COLLECTION
        collection_description = collection_details["description"]

        text_markdown = "# DANRA reanalysis Zarr data collection\n\n"
        text_markdown = f"**{VERSION}, created {datetime.datetime.now().replace(microsecond=0).isoformat()}**\n\n"
        text_markdown += f"> {collection_description}\n\n"
        inputs = self.input()

        for part_id, part_input in self.input().items():
            ds_part = inputs[part_id].open()
            text_markdown += f"## {part_id.replace('_', ' ')}\n\n"
            text_markdown += f"filename: {part_input.path}\n\n"
            if "level" in ds_part.coords:
                N_levels = len(ds_part.level.values)
                var_names = list(ds_part.data_vars)
                N_vars = len(var_names)
                df = pd.DataFrame(
                    columns=var_names,
                    index=ds_part.level.values,
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

        readme_path = self.output()["README"].path

        with open(readme_path, "w") as f:
            f.write(text_markdown)

        logger.info(f"Wrote README file to {readme_path}")

    def output(self):
        fn = "README.md"
        fp_root = FP_ROOT / "data" / VERSION

        outputs = dict(self.input())
        outputs["README"] = ZarrTarget(fp_root / fn)
        return outputs
