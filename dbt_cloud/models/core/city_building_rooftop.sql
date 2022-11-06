{{ config(materialized='table') }}

with fact_building_rooftop as (
    select * from {{ ref('fact_building_rooftop') }}
)
SELECT 
    city,
    year,
    state,
    footprint_m2 AS developable_footprint_area,
    flatarea_m2 AS flat_area_of_developable_plane,
    bldg_fid
FROM fact_building_rooftop
WHERE
(city = 'Albany' AND year = 2013)
OR (city = 'Arnold' AND year = 2006)
OR (city = 'Austin' AND year = 2012)
OR (city = 'Anaheim' AND year = 2012)
OR (city = 'Atlanta' AND year = 2013)
OR (city = 'Augusta' AND year = 2010)
OR (city = 'Baton Rouge' AND year = 2012)
OR (city = 'Albuquerque' AND year = 2012)
OR (city = 'Allentown' AND year = 2006)
OR (city = 'Amarillo' AND year = 2008)

