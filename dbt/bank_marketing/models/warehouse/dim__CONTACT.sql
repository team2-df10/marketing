{{ config(materialized='view') }}

WITH dim__CONTACT AS (
    SELECT DISTINCT
        contact
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_CONTACT('contact') }} as ID_CONTACT, *
FROM 
    dim__CONTACT
order by ID_CONTACT asc