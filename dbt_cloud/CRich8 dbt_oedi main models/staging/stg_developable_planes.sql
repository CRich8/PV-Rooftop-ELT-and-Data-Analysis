{{ config(materialized='view') }}

SELECT
CAST(gid as INTEGER) as gid,
CAST(city as STRING) as city,
CAST(bldg_fid as INTEGER) as bldg_fid,
CAST(footprint_m2 as NUMERIC) as footprint_m2,
CAST(flatarea_m2 as NUMERIC) as flatarea_m2,
CAST(slopeconversion as NUMERIC) as slopeconversion,
CAST(slopearea_m2 as NUMERIC) as slopearea_m2,
CAST(aspect as INTEGER) as aspect

FROM {{ source('staging','developable_planes') }}