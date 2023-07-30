{{ 
  config(
    materialized = 'view',
  ) 
}}

SELECT 
    ft.duration AS Contact_Duration,
    ft.subcribed
FROM {{ ref('fact__tables') }} ft
WHERE ft.duration IS NOT NULL
GROUP BY Contact_Duration, ft.subcribed
ORDER BY ft.subcribed DESC
