{{ config(materialized='view') }}

WITH dim__CREDIT AS (
    SELECT DISTINCT
        credit
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_CREDIT('credit') }} as ID_CREDIT, *
FROM 
    dim__CREDIT