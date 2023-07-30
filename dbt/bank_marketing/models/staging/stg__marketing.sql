{{ config(materialized='view') }}

SELECT     
    *
FROM {{ source('final_project', 'marketing') }}