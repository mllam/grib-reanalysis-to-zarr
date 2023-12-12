import shutil
import tempfile
from pathlib import Path

import fsspec
import rechunker
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata

import dmidc.harmonie


def main(fp_out, rechunk_to):
    tempdir = tempfile.TemporaryDirectory()
    # remove fp_out directory if it exists
    if Path(fp_out).is_dir():
        shutil.rmtree(fp_out)

    fs = fsspec.filesystem("file")

    var_levels = {0: "pres", 2: "t", 10: ["u", "v"]}

    datasets = []

    for level, var_names in var_levels.items():
        ds = dmidc.harmonie.load(
            analysis_time="2013-09-01",
            suite_name="DANRA",
            data_kind="ANALYSIS",
            temp_filepath=tempdir.name,
            short_name=var_names,
            level_type="heightAboveGround",
            level=level,
        )
        datasets.append(ds)

    ds = xr.merge(datasets)

    mapper = fs.get_mapper(fp_out)

    target_chunks = {}
    for d in ds.dims:
        target_chunks[d] = None
    for v in ds.data_vars:
        target_chunks[v] = {d: rechunk_to.get(d) for d in ds[v].dims}
    for c in ds.coords:
        target_chunks[c] = {d: rechunk_to.get(d) for d in ds[c].dims}

    target_store = mapper

    with tempfile.TemporaryDirectory() as temp_dir:

        r = rechunker.rechunk(
            ds,
            target_chunks=target_chunks,
            max_mem="1GB",
            target_store=target_store,
            temp_store=temp_dir,
        )
        r.execute()
        logger.info(r)

    # we have to explicitly consolidate the metadata, since rechunker doesn't do it for us
    # https://github.com/pangeo-data/rechunker/issues/114#issuecomment-1122671510
    # we do it here to save time at load-time (unconsolidated data requires the backend to list all the chunks)

    consolidate_metadata(mapper)

    logger.info("done!", flush=True)


if __name__ == "__main__":
    fp_out = "example_danra_data.zarr"
    rechunk_to = dict(time=4, x=512, y=512)
    main(fp_out=fp_out, rechunk_to=rechunk_to)
