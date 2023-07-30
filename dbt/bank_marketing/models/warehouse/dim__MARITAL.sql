{{ config(materialized='view') }}

WITH dim__MARITAL AS (
    SELECT DISTINCT
        marital
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_MARITAL('marital') }} as ID_MARITAL, *
FROM 
    dim__MARITAL