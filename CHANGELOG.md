# Unreleased

*added*

- license (CC-BY-4.0) and contact information

- limit dataset query to `dmidc.harmonie.load(...)` to avoid including timestep at end of interval and thereby reduce loading time

- handle new time-coordinate structure of dmidc wrt DANRA analysis where the returned xr.Dataset has both `analysis_time` and `time` coordinates

- add CF-compliant projection information to all variables (hardcoded for now, but should be parsed from gribscan/dmidc soon)

- add loading of a set of variables sharing levels in single query to `dmidc.harmone.load(...)` to reduce load time

- update to head for dmidc

*fixes*

- ensure coordinate order for vertical coordinates is maintained


# v0.5.0rc0

*changed*

- intermediate dataset with only 3 most recent years (2019-01-01 to 2024-01-01)

*added*

- README rendered in html as well as markdown

# v0.4.0

*changed*

- increased time period to full 30 years (1990-09-01 to 2020-09-01)

- add option to delete intermediate zarr-files to reduce storage requirements


# v0.3.0

*changed*

- time duration changed from one year (1990-09-01 to 1991-09-1) to ten first years (1990-09-01 to 2000-09-01)

- for pressure and height levels the "level" coordinate has been named `pressure` and `height` respectively

- chunking size increased to `[time,x,y]=(256,256,256)`

*maintenance*

- implement staggered aggregation in time to reduce number of final aggregation tasks

- handle rechunking when chunksize from first aggregations are below the intended chunksize


*fixes*

- add checks for missing timesteps


# v0.2.2

_fixes_

- Fix time-overlap bug that lead to repeated timesteps in output

_maintenance_

- Added more detailed logging including installed versions and script to copy
  completed datasets to scale.dmi.dk


# v0.2.1

First data-collection with all three parts, with variables on 1) height-,
2) pressure- and 3) single-levels (e.g. surface, top-of-atmosphere,
column-integrated, etc). Only covering prognostic variables at 1000hPa pressure
level, 100m height level and single levels, and only for year of 1990.
