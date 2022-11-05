 {{ config(materialized='table') }}

SELECT * FROM `oedi-project.dbt_ckeskin.table_pvsys1430`   UNION ALL 
SELECT * FROM `oedi-project.dbt_ckeskin.table_pvsys1431`   UNION ALL
SELECT * FROM `oedi-project.dbt_ckeskin.table_pvsys1432`   UNION ALL
SELECT * FROM `oedi-project.dbt_ckeskin.table_pvsys1433` 
