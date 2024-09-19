# DANRA in zarr

This repository contains the code to process DANRA GRIB files into `zarr` format and [notebooks](notebooks/) demonstrating how to work with the processed datasets.


## Reading DANRA in zarr

To read the DANRA processed datasets you will need to install the `xarray` and `zarr` packages, e.g. with:

```markdown
python -m pip install xarray zarr
```

Please have a look at the [notebooks](notebooks/) for an overview of how the data is structured and how you use it with `xarray`.


## Processing


Create a full dataset collection for DANRA (remember to edit [danra_to_zarr/pipeline/config.py](danra_to_zarr/pipeline/config.py) to add a configuration for this dataset):

```
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraCompleteZarrCollection
```

Or to just extract one part of the data, e.g. `height_levels`:

```
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrCollection --part-id height_levels
```

Create a single zarr-based subset of DANRA with luigi

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubset --t-start 2020-01-01T0000 --t-end 2020-01-02T0000 --variables '["u", "v", "r", "t"]' --levels "[1000, 900]" --level-type isobaricInhPa
```

If you're aiming to create a zarr-archive for a long time-window then you can use `DanraZarrSubsetAggregated` to create smaller intermediate zarr archives (the time-interval is set with `t_interval`) before aggregating to a single zarr arhive.

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubsetAggregated --t-start 2019-01-01T0000 --t-end 2020-01-01T0000 --t-interval P7D --variables '["u", "v", "r", "t"]' --levels "[1000, 900]" --levevel-type isobaricInhPa

```

### Optimizing the workflow while handling total memory usage

Within the pipeline tasks there are two memory hungry steps:

1. Extracting of the initial time-subset of DANRA as a .zarr-dataset from the source GRIB files

2. Aggregating together subsets (which amounts to a concatenation in time)


## Data overview

See [variables_levels_overview.md](variables_levels_overview.md) for an overview from the source GRIB files of all variables, and the levels and level types they're on.
