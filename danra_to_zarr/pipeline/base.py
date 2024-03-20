import shutil
import tempfile
from pathlib import Path

import dmidc.utils
import isodate
import luigi
import pandas as pd
import xarray as xr
from loguru import logger

from ..main import create_zarr_dataset
from . import logging  # noqa
from .config import DELETE_INTERMEDIATE_ZARR_FILES, FP_ROOT, FP_TEMP_ROOT
from .utils import time_to_str


class ZarrTarget(luigi.Target):
    def __init__(self, fp):
        self.fp = fp

    def exists(self):
        return self.fp.exists()

    def open(self):
        return xr.open_zarr(self.fp)

    def write(self, ds):
        ds.to_zarr(self.path)

    @property
    def path(self):
        return self.fp


class DanraZarrSubset(luigi.Task):
    """
    Create a subset of DANRA into a zarr archive
    """

    t_start = luigi.DateMinuteParameter()
    t_end = luigi.DateMinuteParameter()
    variables = luigi.DictParameter()
    level_type = luigi.Parameter()
    rechunk_to = luigi.DictParameter()
    level_name_mapping = luigi.OptionalDictParameter(default=None)

    def run(self):
        if any([c not in self.rechunk_to for c in ["time", "x", "y"]]):
            raise Exception("rechunk_to should contain time, x and y")

        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        FP_TEMP_ROOT.mkdir(exist_ok=True, parents=True)

        # since we're going to be concatenating datasets along the time dimension
        # we need to ensure that the time dimension is chunked to 1 otherwise
        # dask will complain about concatenating datasets with different chunking
        rechunk_to = dict(self.rechunk_to)
        # rechunk_to["time"] = 1

        identifier = self.identifier
        with tempfile.TemporaryDirectory(
            dir=FP_TEMP_ROOT, prefix=identifier
        ) as tempdir:

            create_zarr_dataset(
                fp_temp=Path(tempdir),
                fp_out=self.output().path,
                analysis_time=self.analysis_time,
                rechunk_to=rechunk_to,
                variables=self.variables,
                level_type=self.level_type,
                level_name_mapping=self.level_name_mapping,
            )

    @property
    def analysis_time(self):
        analysis_time = dmidc.utils.normalise_time_argument(
            slice(self.t_start.isoformat(), self.t_end.isoformat())
        )

        if analysis_time.step is not None:
            raise NotImplementedError("analysis_time.step must be None")

        return analysis_time

    @property
    def identifier(self):
        analysis_time = self.analysis_time
        variables_identifier_parts = [
            f"{var_name}_{'_'.join(str(l) for l in levels)}"
            for (var_name, levels) in self.variables.items()
        ]
        name_parts = [
            "danra",
            self.level_type,
            "_".join(variables_identifier_parts),
            f"{time_to_str(analysis_time.start)}-{time_to_str(analysis_time.stop)}",
        ]
        identifier = ".".join(name_parts)
        return identifier

    def output(self):
        fn = f"{self.identifier}.zarr"
        fp = FP_ROOT / "subset" / fn

        return ZarrTarget(fp)


class DanraZarrSubsetAggregated(DanraZarrSubset):
    """
    Aggregate multiple zarr based subsets of DANRA into a single zarr archive.
    To reduce the number of subsets that need to be aggregated `t_intervals` should define
    a list of time-durations that should be aggregated over, e.g.
    `t_intervals=["PT24H", "P7D", "P1Y"]` will aggregate first into 24-hour intervals,
    then these into 1-week intervals, these into 1-year intervals
    and finally into the complete dataset.
    """

    t_intervals = luigi.ListParameter()

    def requires(self):
        t_intervals = list(self.t_intervals)
        t_interval = isodate.parse_duration(t_intervals.pop(-1))

        create_child_aggregate = len(t_intervals) > 0

        ts = pd.date_range(self.t_start, self.t_end, freq=t_interval, inclusive="both")

        tasks = []
        for t_start, t_end in zip(ts[:-1], ts[1:]):
            kwargs = dict(
                t_start=t_start,
                t_end=t_end,
                variables=self.variables,
                level_type=self.level_type,
                rechunk_to=self.rechunk_to,
                level_name_mapping=self.level_name_mapping,
            )
            if create_child_aggregate:
                task = self.__class__(t_intervals=t_intervals, **kwargs)
            else:
                task = DanraZarrSubset(**kwargs)
            tasks.append(task)

        return tasks

    def run(self):
        datasets = []
        inputs = self.input()
        for i, inp in enumerate(inputs):
            try:
                ds = inp.open().reset_encoding()
            except Exception as ex:
                raise Exception(f"There was an exception opening {inp.path}: {ex}")
            datasets.append(ds)

        ds = xr.concat(datasets, dim="time").chunk(self.rechunk_to)
        # check that all time increments are the same, this will check for gaps
        # as well as duplicates in the data
        da_dt = ds.time.diff(dim="time")
        if not da_dt.min() == da_dt.max():
            raise Exception(
                "Not all time increments are the same in the concatenated data."
                " Maybe some timesteps are duplicated or missing? Tried to combine"
                f" datasets from the following paths: {' '.join(map(lambda t: str(t.path), inputs))}"
            )
        self.output().write(ds)
        logger.info(f"{self.output().path} done!", flush=True)

        if DELETE_INTERMEDIATE_ZARR_FILES:
            fps_parents = [inp.path for inp in inputs]
            logger.info(f"Deleting input source files: {fps_parents}")
            for fp_parent in fps_parents:
                shutil.rmtree(fp_parent)
