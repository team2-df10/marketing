{{ 
  config(
    materialized = 'view'
  ) 
}}
SELECT 
    ft.age,
    dj.job AS Job,
    dm.marital AS Marital_Status,
    de.education AS Education_Level,
    dc.credit AS Has_Credit_Default,
    dh.housing AS Has_Housing_Loan,
    dl.loan AS Has_Personal_Loan,
    ft.subcribed AS Subcribed

FROM {{ ref('fact__tables') }} ft
LEFT JOIN {{ ref('dim__JOB') }} dj ON ft.ID_JOB = dj.ID_JOB
LEFT JOIN {{ ref('dim__MARITAL') }} dm ON ft.ID_MARITAL = dm.ID_MARITAL
LEFT JOIN {{ ref('dim__EDUCATION') }} de ON ft.ID_EDUCATION = de.ID_EDUCATION
LEFT JOIN {{ ref('dim__CREDIT') }} dc ON ft.ID_CREDIT = dc.ID_CREDIT
LEFT JOIN {{ ref('dim__HOUSING') }} dh ON ft.ID_HOUSING = dh.ID_HOUSING
LEFT JOIN {{ ref('dim__LOAN') }} dl ON ft.ID_LOAN = dl.ID_LOAN

GROUP BY ft.age, Job, Marital_Status, Education_Level, Has_Credit_Default, Has_Housing_Loan, Has_Personal_Loan, Subcribed
ORDER BY Subcribed DESC