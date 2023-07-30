{{ config(materialized='view') }}

WITH dim__EDUCATION AS (
    SELECT DISTINCT
        education
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_EDUCATION('education') }} as ID_EDUCATION, *
FROM 
    dim__EDUCATION