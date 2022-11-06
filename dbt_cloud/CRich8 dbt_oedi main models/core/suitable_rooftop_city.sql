{{ config(materialized='table') }}

with fact_building_rooftop as (
    select * from {{ ref('fact_building_rooftop') }}
)
    SELECT 
    city,
    year,
    state,

    sum(footprint_m2) as total_developable_plane_footprint_area,
    sum(flatarea_m2) as total_developable_plane_flat_area,
    count(bldg_fid) as building_count

    FROM fact_building_rooftop
    GROUP BY 1,2,3