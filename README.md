# Processing DANRA grib files to zarr


## Usage

Create a full dataset collection for DANRA (remember to edit [danra_to_zarr/pipeline/config.py](danra_to_zarr/pipeline/config.py) to add a configuration for this dataset):

```
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraCompleteZarrCollection --version v0.1.0
```

Create a single zarr-based subset of DANRA with luigi

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubset --t-start 2020-01-01T0000 --t-end 2020-01-02T0000 --variables '["u", "v", "r", "t"]' --levels "[1000, 900]" --level-type isobaricInhPa
```

If you're aiming to create a zarr-archive for a long time-window then you can use `DanraZarrSubsetAggregated` to create smaller intermediate zarr archives (the time-interval is set with `t_interval`) before aggregating to a single zarr arhive.

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubsetAggregated --t-start 2019-01-01T0000 --t-end 2020-01-01T0000 --t-interval P7D --variables '["u", "v", "r", "t"]' --levels "[1000, 900]" --levevel-type isobaricInhPa

```


## Data overview

See [variables_levels_overview.md](variables_levels_overview.md) for an overview of all variables, and the levels and level types they're on.

### Steps for extraction:

1. Specify what variables on what levels of what level-types to extract
2. Specify what time-period to extract
3. Specify chunking
4. Specify where to store extracted zarr dataset


# intake catalog structure for danra

```yaml
description: "DANRA reanalysis data zarr extraction datasets"

sources:
  danra:
    description: "DANRA reanalysis data zarr extraction datasets"
    driver: intake_xarray.xzarr.ZarrSource
    args:
      urlpath: "https://scale-s3.dmi.dk/danra/{version}/{part_id}.zarr"
      storage_options:
        anon: true
    metadata:
      part_id:
        - height_levels
        - pressure_levels
        - single_levels
      version:
        - v0.1.0
```

```python
import intake

cat_url = "https://scale-s3.dmi.dk/danra/catalog.yml"
ds_height_levels = intake.open_catalog(cat_url).to_dask(version="v0.1.0", part_id="height_levels")
da_u = ds_height_levels.u
```
