import shutil
import tempfile
from pathlib import Path

import dmidc.harmonie
import fsspec
import rechunker
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata


def main(fp_temp, fp_out, rechunk_to, level_type, variables, levels=None):
    """
    Produce a zarr store with the given variables and levels, rechunked to the given chunk sizes.

    Parameters
    ----------
    fp_out : str
        File path to the output zarr store
    rechunk_to : dict
        Dictionary specifying the chunk sizes to rechunk to. Keys are dimension names, values are chunk sizes.
        e.g. {'time': 4, 'x': 512, 'y': 512}
    level_type : str
        Level type to load, e.g. 'isobaricInhPa' or 'heightAboveGround'
    variables : dict or str
        Dictionary of variables to load, e.g. {'t': [850, 500]} or ['t', 'w']
        if a dict, levels must be None
    levels : list or None
        List of levels to load, e.g. [850, 500]
        if a list, variables must be a list of variable names
    """
    # remove fp_out directory if it exists
    if Path(fp_out).is_dir():
        shutil.rmtree(fp_out)

    fs = fsspec.filesystem("file")

    def _get_levels_and_variables():
        if isinstance(variables, dict):
            if levels is not None:
                raise ValueError("if variables is a dict, levels must be None")

            for var_name, var_levels in variables.items():
                for level in var_levels:
                    yield level, var_name
        elif isinstance(variables, list) and isinstance(levels, list):
            for level in levels:
                yield level, variables
        else:
            raise NotImplementedError(variables, levels)

    datasets = []

    for level, var_names in _get_levels_and_variables():
        logger.info(f"loading {var_names} on {level_type} level {level}")
        ds = dmidc.harmonie.load(
            analysis_time="2013-09-01",
            suite_name="DANRA",
            data_kind="ANALYSIS",
            temp_filepath=fp_temp,
            short_name=var_names,
            level_type=level_type,
            level=level,
        )
        ds.coords["level"] = level
        datasets.append(ds)

    ds = xr.concat(datasets, dim="level")

    # TODO: this should really be done in dmidc.harmonie.load
    if level_type == "isobaricInhPa":
        ds.coords["level"].attrs["units"] = "hPa"
    else:
        raise NotImplementedError(level_type)

    mapper = fs.get_mapper(fp_out)

    target_chunks = {}
    for d in ds.dims:
        target_chunks[d] = [rechunk_to.get(d, ds[d].size)]
    for c in ds.coords:
        # target_chunks[c] = {d: target_chunks[d] for d in ds[c].dims}
        target_chunks[c] = {d: rechunk_to.get(d, ds[d].size) for d in ds[c].dims}
    for v in ds.data_vars:
        # target_chunks[v] = {d: target_chunks[d] for d in ds[v].dims}
        target_chunks[v] = {d: rechunk_to.get(d, ds[d].size) for d in ds[v].dims}

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

    # we have to explicitly consolidate the metadata, since rechunker doesn't do it for us
    # https://github.com/pangeo-data/rechunker/issues/114#issuecomment-1122671510
    # we do it here to save time at load-time (unconsolidated data requires the backend to list all the chunks)

    consolidate_metadata(mapper)

    ds_zarr = xr.open_zarr(mapper)
    logger.info(ds_zarr)

    logger.info("done!", flush=True)


if __name__ == "__main__":
    level_type = "isobaricInhPa"
    fp_root = Path("/dmidata/projects/cloudphysics/danra")
    fp_tempfiles = fp_root / "tempfiles"
    fp_out = fp_root / "data" / f"{level_type}.zarr"
    kwargs = dict(variables=["t", "u", "v", "r"], levels=[1000, 900])
    rechunk_to = dict(time=4, x=512, y=512)

    import ipdb

    with ipdb.launch_ipdb_on_exception():
        main(
            fp_temp=fp_tempfiles,
            fp_out=fp_out,
            rechunk_to=rechunk_to,
            level_type=level_type,
            **kwargs,
        )
