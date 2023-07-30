{{ 
  config(
    materialized = 'view'
  ) 
}}

SELECT 
    icr.credit AS Has_Credit_Default,
    ih.housing AS Has_Housing_Loan,
    il.loan AS Has_Personal_Loan,
    COUNT(*) as Number_of_Clients,
    SUM(CASE WHEN ft.subcribed = 1 THEN 1 ELSE 0 END) AS subs,
    SUM(CASE WHEN ft.subcribed = 0 THEN 1 ELSE 0 END) AS not_subs
FROM {{ ref('stg__marketing') }} ft
LEFT JOIN {{ ref('dim__CREDIT') }} icr ON ft.credit = icr.credit
LEFT JOIN {{ ref('dim__HOUSING') }} ih ON ft.housing = ih.housing
LEFT JOIN {{ ref('dim__LOAN') }} il ON ft.loan = il.loan
GROUP BY 1, 2, 3
