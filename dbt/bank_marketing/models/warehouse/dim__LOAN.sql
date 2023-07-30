{{ config(materialized='view') }}

WITH dim__LOAN AS (
    SELECT DISTINCT
        loan
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_LOAN('loan') }} as ID_LOAN, *
FROM 
    dim__LOAN