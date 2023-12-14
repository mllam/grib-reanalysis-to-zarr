import tempfile
from pathlib import Path

import dmidc.utils
import luigi

from .main import create_zarr_dataset

FP_ROOT = Path("/dmidata/projects/cloudphysics/danra")
RECHUNK_TO = dict(time=4, x=512, y=512)


def _time_to_str(t):
    return t.isoformat().replace(":", "").replace("-", "")


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
            "_".join(self.variables),
            "_".join([str(level) for level in self.levels]),
            f"{_time_to_str(analysis_time.start)}-{_time_to_str(analysis_time.stop)}",
        ]
        identifier = ".".join(name_parts)
        return identifier

    def output(self):
        fn = f"{self.identifier}.zarr"

        return luigi.LocalTarget(FP_ROOT / "data" / fn)
