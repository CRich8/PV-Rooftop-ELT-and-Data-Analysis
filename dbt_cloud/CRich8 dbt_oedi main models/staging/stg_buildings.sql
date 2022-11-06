{{ config(materialized='view') }}

SELECT
CAST(gid as INTEGER) as gid,
CAST(bldg_fid as INTEGER) as bldg_fid,
CAST(the_geom_96703 as STRING ) as the_geom_96703,
CAST(the_geom_4326 as STRING) as the_geom_4326,
CAST(city AS STRING) as city,
CAST(state as STRING) as state,
CAST(year AS INTEGER)  as year

FROM {{ source('staging','buildings') }}