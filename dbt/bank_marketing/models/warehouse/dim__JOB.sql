{{ config(materialized='view') }}

WITH dim__JOB AS (
    SELECT DISTINCT
        job
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ encode_JOB('job') }} as ID_JOB, *
FROM 
    dim__JOB
