{{ 
  config(
    materialized = 'view'
  ) 
}}

SELECT 
    ft.emp_var_rate AS Employment_Variation_Rate,
    ft.cons_price_idx AS Consumer_Price_Index,
    ft.cons_conf_idx AS Consumer_Confidence_Index,
    ft.euribor3m AS Euribor_Rate,
    ft.nr_employed AS Number_Employed,
    ft.subcribed
FROM {{ ref('fact__tables') }} ft
ORDER BY subcribed DESC