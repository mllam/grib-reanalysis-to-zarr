import isodate
import luigi
import xarray as xr
from loguru import logger

from .base import DanraZarrSubsetAggregated, ZarrTarget
from .config import DATA_COLLECTIONS, FP_ROOT


class DanraZarrCollection(luigi.Task):
    """
    Create one part of DANRA dataset for a specific version using the mappings from
    level-type, variable name and levels to collection parts defined in DATA_COLLECIONS
    """

    version = luigi.Parameter()
    part_id = luigi.Parameter()

    def requires(self):
        collection_details = DATA_COLLECTIONS[self.version]
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
        collection_details = DATA_COLLECTIONS[self.version]
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
            f"Writing {self.version} collection part {part_id} to {part_output.path}"
        )
        part_output.write(ds_part)

    def output(self):
        path_root = FP_ROOT / "data" / self.version
        fn = f"{self.part_id}.zarr"
        return ZarrTarget(path_root / fn)


class DanraCompleteZarrCollection(luigi.WrapperTask):
    """
    Create full DANRA dataset for a specific version using the mappings from
    level-type, variable name and levels to collection parts defined in DATA_COLLECIONS
    """

    version = luigi.Parameter()

    def requires(self):
        collection_details = DATA_COLLECTIONS[self.version]

        tasks = {}

        for part_id in collection_details["parts"].keys():
            tasks[part_id] = DanraZarrCollection(version=self.version, part_id=part_id)

        return tasks
