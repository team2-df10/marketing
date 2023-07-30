{{ config(materialized='view') }}

WITH dim__HOUSING AS (
    SELECT DISTINCT
        housing
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_HOUSING('housing') }} as ID_HOUSING, *
FROM 
    dim__HOUSING