# Processing DANRA grib files to zarr

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

