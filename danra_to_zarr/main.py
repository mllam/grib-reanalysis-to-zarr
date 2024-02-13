import shutil
from pathlib import Path

try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

import warnings

import dmidc.harmonie
import dmidc.utils
import fsspec
import rechunker
import xarray as xr
from loguru import logger
from zarr.convenience import consolidate_metadata


def _is_listlike(v):
    return isinstance(v, list) or isinstance(v, tuple)


def create_zarr_dataset(
    fp_temp,
    fp_out,
    analysis_time,
    rechunk_to,
    level_type,
    variables,
    levels=None,
    level_name_mapping=None,
):
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
    level_name_mapping : str or None
        String to use to format the variable with a given level type
        e.g. "{var_name}{level}hPa", "{var_name}{level}m" where var_name is the
        variable name and level is the level value
    """
    # remove fp_out directory if it exists
    if Path(fp_out).is_dir():
        shutil.rmtree(fp_out)

    def _get_levels_and_variables():
        # need to match `dict` and `luigi.freezing.FrozenOrderedDict` which both subclass Mapping
        if isinstance(variables, Mapping):
            if levels is not None:
                raise ValueError("if variables is a dict, levels must be None")

            # group the variables so that we can load them all at once
            # e.g. {'t': [850, 500], 'w': [850, 500, 100]} -> {850: ['t', 'w'], 500: ['t', 'w'], 100: ['w']}

            # first, get a list of all the levels
            all_levels = []
            for var_name, var_levels in variables.items():
                all_levels.extend(var_levels)
            all_levels = list(set(all_levels))

            # now, for each level, get the variables that are defined for that level
            for level in all_levels:
                var_names = []
                for var_name, var_levels in variables.items():
                    if level in var_levels:
                        var_names.append(var_name)
                yield level, var_names

        elif _is_listlike(variables) and _is_listlike(levels):
            for level in levels:
                yield level, variables
        else:
            raise NotImplementedError(variables, levels)

    datasets = []

    for level, var_names in _get_levels_and_variables():
        logger.info(f"loading {var_names} on {level_type} level {level}")
        try:
            ds = dmidc.harmonie.load(
                analysis_time=analysis_time,
                suite_name="DANRA",
                data_kind="ANALYSIS",
                temp_filepath=fp_temp,
                short_name=var_names,
                level_type=level_type,
                level=level,
            )
        except Exception as ex:
            logger.error(ex)
            raise

        if level_name_mapping is None:
            ds.coords["level"] = level
        else:
            for var_name in var_names:
                da_var = ds[var_name]
                ds = ds.drop(var_name)
                da_var.attrs["level"] = level
                new_var_name = level_name_mapping.format(var_name=var_name, level=level)
                ds[new_var_name] = da_var

        datasets.append(ds)

    if level_name_mapping is None:
        ds = xr.concat(datasets, dim="level")
    else:
        ds = xr.merge(datasets)

    if level_name_mapping is None:
        # TODO: this should really be done in dmidc.harmonie.load
        if level_type == "isobaricInhPa":
            ds.coords["level"].attrs["units"] = "hPa"
        elif level_type in ["heightAboveGround", "heightAboveSea"]:
            ds.coords["level"].attrs["units"] = "m"
        elif level_type in ["entireAtmosphere", "nominalTop", "surface"]:
            pass
        else:
            raise NotImplementedError(level_type)

    mapper = rechunk_and_write(
        ds=ds, fp_out=fp_out, fp_temp=fp_temp, rechunk_to=rechunk_to
    )

    ds_zarr = xr.open_zarr(mapper)
    logger.info(ds_zarr)

    logger.info(f"{fp_out} done!", flush=True)


def rechunk_and_write(ds, fp_temp, fp_out, rechunk_to):
    fs = fsspec.filesystem("file")
    mapper = fs.get_mapper(fp_out)

    for d in ds.dims:
        dim_len = len(ds[d])
        if d in rechunk_to and rechunk_to[d] > dim_len:
            warnings.warn(
                f"Requested chunksize for dim `{d}` is larger than then dimension"
                f" size ({rechunk_to[d]} > {dim_len}). Reducing to dimension size."
            )
            rechunk_to[d] = dim_len

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

    r = rechunker.rechunk(
        ds,
        target_chunks=target_chunks,
        max_mem="32GB",
        target_store=target_store,
        temp_store=fp_temp,
    )
    r.execute()

    # we have to explicitly consolidate the metadata, since rechunker doesn't do it for us
    # https://github.com/pangeo-data/rechunker/issues/114#issuecomment-1122671510
    # we do it here to save time at load-time (unconsolidated data requires the backend to list all the chunks)

    consolidate_metadata(mapper)

    return mapper


if __name__ == "__main__":
    level_type = "isobaricInhPa"
    fp_root = Path("/dmidata/projects/cloudphysics/danra")
    fp_tempfiles = fp_root / "tempfiles"
    kwargs = dict(variables=["t", "u", "v", "r"], levels=[1000, 900])
    rechunk_to = dict(time=4, x=512, y=512)
    analysis_time = dmidc.utils.normalise_time_argument(
        slice("2021-08-01T00:00", "2021-09-01T00:00")
    )

    def _time_to_str(t):
        return t.isoformat().replace(":", "").replace("-", "")

    if analysis_time.step is not None:
        raise NotImplementedError("analysis_time.step must be None")

    name_parts = [
        "danra",
        level_type,
        f"{_time_to_str(analysis_time.start)}-{_time_to_str(analysis_time.stop)}",
    ]

    fn = f"{'_'.join(name_parts)}.zarr"

    fp_out = fp_root / "data" / fn

    import ipdb

    with ipdb.launch_ipdb_on_exception():
        create_zarr_dataset(
            fp_temp=fp_tempfiles,
            fp_out=fp_out,
            analysis_time=analysis_time,
            rechunk_to=rechunk_to,
            level_type=level_type,
            **kwargs,
        )
