import datetime
import tempfile
from pathlib import Path

import dmidc.utils
import luigi
import pandas as pd
import xarray as xr
from loguru import logger

from .main import create_zarr_dataset

FP_ROOT = Path("/dmidata/projects/cloudphysics/danra")
RECHUNK_TO = dict(time=4, x=512, y=512)


def _time_to_str(t):
    return t.isoformat().replace(":", "").replace("-", "")


fn_log = f"{_time_to_str(datetime.datetime.now())}.log"
logger.add(fn_log)
logger.info(f"Logging to {fn_log}")


class ZarrTarget(luigi.Target):
    def __init__(self, fp):
        self.fp = fp

    def exists(self):
        return self.fp.exists()

    def open(self):
        return xr.open_zarr(self.fp)

    @property
    def path(self):
        return self.fp


class DanraZarrSubset(luigi.Task):
    """
    Create a subset of DANRA into a zarr archive
    """

    t_start = luigi.DateMinuteParameter()
    t_end = luigi.DateMinuteParameter()
    variables = luigi.ListParameter()
    levels = luigi.ListParameter()
    level_type = luigi.Parameter()

    def run(self):

        identifier = self.identifier
        tempdir = tempfile.TemporaryDirectory(
            dir=FP_ROOT / "tempfiles", prefix=identifier
        )

        create_zarr_dataset(
            fp_temp=Path(tempdir.name),
            fp_out=self.output().path,
            analysis_time=self.analysis_time,
            rechunk_to=RECHUNK_TO,
            variables=list(self.variables),
            levels=list(self.levels),
            level_type=self.level_type,
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
        name_parts = [
            "danra",
            self.level_type,
            "_".join(sorted(self.variables)),
            "_".join([str(level) for level in sorted(self.levels)]),
            f"{_time_to_str(analysis_time.start)}-{_time_to_str(analysis_time.stop)}",
        ]
        identifier = ".".join(name_parts)
        return identifier

    def output(self):
        fn = f"{self.identifier}.zarr"

        return ZarrTarget(FP_ROOT / "data" / fn)


class DanraZarrSubsetAggregated(DanraZarrSubset):
    """
    Aggregate multiple zarr based subsets of DANRA into a single zarr archive
    """

    t_interval = luigi.TimeDeltaParameter()

    def requires(self):
        # if the timespan is greater than a year then we need to ensure that we
        # always start on the first of january so that we can reuse blocks of
        # data we're already done

        t0 = self.t_start

        ts = []
        while t0 < self.t_end:
            if t0.year != self.t_end.year:
                t_startof_next_year = datetime.datetime(t0.year + 1, 1, 1)
                ts += list(
                    pd.date_range(
                        t0, t_startof_next_year, freq=self.t_interval, inclusive="both"
                    )
                )
                t0 = t_startof_next_year
            else:
                ts += list(
                    pd.date_range(
                        t0, self.t_end, freq=self.t_interval, inclusive="both"
                    )
                )
                t0 = self.t_end

        tasks = []
        for t_start, t_end in zip(ts[:-1], ts[1:]):
            task = DanraZarrSubset(
                t_start=t_start,
                t_end=t_end,
                variables=self.variables,
                levels=self.levels,
                level_type=self.level_type,
            )
            tasks.append(task)

        return tasks

    def run(self):
        datasets = [inp.open() for inp in self.input()]
        ds = xr.concat(datasets, dim="time").chunk(RECHUNK_TO)
        ds.to_zarr(self.output().path)
        logger.info(f"{self.output().path} done!", flush=True)
