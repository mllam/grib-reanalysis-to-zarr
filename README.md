# Processing DANRA grib files to zarr


## Usage

Create a single zarr-based subset of DANRA with luigi

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubset --t-start 2020-01-01T0000 --t-end 2020-01-02T0000 --variables '["u", "v"]' --levels "[1000, 900]" --level-type isobaricInhPa
```

If you're aimiing to create a zarr-archive for a long time-window then you can use `DanraZarrSubsetAggregated` to create smaller intermediate zarr archives (the time-interval is set with `t_interval`) before aggregating to a single zarr arhive.

```bash
PYTHONPATH=`pwd`:$PYTHONPATH pdm run luigi --module danra_to_zarr.pipeline DanraZarrSubset --t-start 20191-01T0000 --t-end 2020-01-01T0000 - --t-interval PT7D -variables '["u", "v"]' --levels "[1000, 900]" --levevel-type isobaricInhPa

```



```mermaid
graph TB

```


```yaml
level_types:
  5: adiabaticCondensation
  8: nominalTop
  20: unknown
  100: isobaricInhPa
  103: heightAboveSea
  105: heightAboveGround
  200: entireAtmosphere
```

### Questions to answer:

1. Do we want to include all level-types?
2. What is level-type with id 20?

How to build up extraction process over smaller steps:

- what would be a complete extraction?
    - [ ] create table with all variables and levels for each level-type
- how longer does it take to do a complete extraction for say a full days worth of data?

### Steps for extraction:

1. Specify what variables on what levels of what level-types to extract
2. Specify what time-period to extract
3. Specify chunking
4. Specify where to store extracted zarr dataset


# intake catalog structure for danra

```yaml
description: "DANRA reanalysis data zarr extraction datasets"

sources:
  danra_2levels_1990to1991:
    description: "DANRA reanalysis data zarr extraction datasets"
    driver: intake_xarray.xzarr.ZarrSource
    args:
      urlpath: "https://scale-s3.dmi.dk/danra_2levels_1990to1991/{level_type}.zarr"
      storage_options:
        anon: true
    metadata:
      level_types:
        - adiabaticCondensation
        - nominalTop
        - isobaricInhPa
        - heightAboveSea
        - heightAboveGround
        - entireAtmosphere
```
root:

    - danra_2levels_1990to1991
    - danra_full

```python
import intake

cat_url = "https://scale-s3.dmi.dk/danra/catalog.yml"
ds_height_above_ground = intake.open_catalog(cat_url).to_dask(level_type="heightAboveGround")
da_w = ds_height_above_ground.w
```
