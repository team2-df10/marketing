{{ config(materialized='view') }}

WITH dim__POUTCOME AS (
    SELECT DISTINCT
        poutcome
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_POUTCOME('poutcome') }} as ID_POUTCOME, *
FROM 
    dim__POUTCOME