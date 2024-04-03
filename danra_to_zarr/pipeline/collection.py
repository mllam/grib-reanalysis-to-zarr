import datetime
import io
import shutil

import isodate
import luigi
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata

from ..utils.print_versions import show_versions
from .base import DanraZarrSubsetAggregated, ZarrTarget
from .config import DATA_COLLECTION, DELETE_INTERMEDIATE_ZARR_FILES, FP_ROOT, VERSION


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

            level_name_mapping = collection_details["parts"][part_id][level_type].get(
                "level_name_mapping"
            )
            task = DanraZarrSubsetAggregated(
                t_start=isodate.parse_date(timespan.start),
                t_end=isodate.parse_date(timespan.stop),
                t_intervals=collection_details["intermediate_time_partitioning"],
                variables=level_type_variables["variables"],
                level_type=level_type,
                rechunk_to=collection_details["rechunk_to"],
                level_name_mapping=level_name_mapping,
            )
            tasks[level_type] = task

        return tasks

    def run(self):
        collection_details = DATA_COLLECTION
        collection_description = collection_details["description"]

        part_id = self.part_id
        part_inputs = self.input()

        part_contents = {}
        for level_type, level_type_inputs in part_inputs.items():
            ds_level_type = level_type_inputs.open()
            for var_name in ds_level_type.data_vars:
                da_var = ds_level_type[var_name]
                da_var.attrs["level_type"] = level_type
                part_contents[var_name] = da_var

        ds_part = xr.Dataset(part_contents)

        ds_part.attrs["description"] = collection_description

        part_output = self.output()
        part_output.path.parent.mkdir(exist_ok=True, parents=True)
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
                "paramId",
            ]:
                if attr in ds_part[var_name].attrs:
                    del ds_part[var_name].attrs[attr]
            ds_part[var_name].attrs["long_name"] = ds_part[var_name].attrs.pop("name")
            ds_part[var_name] = ds_part[var_name]

        part_output.write(ds_part)
        consolidate_metadata(part_output.path)

        if DELETE_INTERMEDIATE_ZARR_FILES:
            fps_parents = [inp.path for inp in part_inputs.values()]
            logger.info(f"Deleting input source files: {fps_parents}")
            for fp_parent in fps_parents:
                shutil.rmtree(fp_parent)

    def output(self):
        path_root = FP_ROOT / VERSION
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
        text_markdown += f"**{VERSION}, created {datetime.datetime.now().replace(microsecond=0).isoformat()}**\n\n"
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
                var_names = list(ds_part.data_vars)
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

    def output(self):
        fn = "README.md"
        fp_root = FP_ROOT / VERSION

        outputs = dict(self.input())
        outputs["README"] = ZarrTarget(fp_root / fn)
        return outputs
