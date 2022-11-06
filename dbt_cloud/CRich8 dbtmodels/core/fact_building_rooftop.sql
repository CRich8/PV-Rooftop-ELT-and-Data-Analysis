{{ config(materialized='table') }}

with buildings as (
    SELECT * 
    FROM {{ ref('stg_buildings') }}

),

developable_planes as (
    SELECT *
    FROM {{ ref('stg_developable_planes') }}
)

SELECT
    buildings.bldg_fid,
    developable_planes.gid,
    buildings.city,
    buildings.year,
    buildings.state,
    developable_planes.footprint_m2,
    developable_planes.flatarea_m2,
    developable_planes.slopeconversion,
    developable_planes.slopearea_m2,
    developable_planes.aspect,
    buildings.the_geom_96703,
    buildings.the_geom_4326

FROM buildings
INNER JOIN developable_planes
ON buildings.bldg_fid = developable_planes.bldg_fid AND buildings.city = developable_planes.city

