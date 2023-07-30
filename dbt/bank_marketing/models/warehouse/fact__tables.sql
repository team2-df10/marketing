{{ config(materialized='view') }}

WITH fact__tables AS (
    SELECT * 
    FROM {{ ref('stg__marketing') }}
)

SELECT
    ft.client_id,
    ft.age,
    ij.ID_JOB,
    im.ID_MARITAL,
    ie.ID_EDUCATION,
    icr.ID_CREDIT,
    ih.ID_HOUSING,
    il.ID_LOAN,
    ic.ID_CONTACT,
    ft.month,
    ft.day_of_week,
    ft.duration,
    ft.campaign,
    ft.pdays,
    ft.previous,
    ip.ID_POUTCOME,
    ft.emp_var_rate,
    ft.cons_price_idx,
    ft.cons_conf_idx,
    ft.euribor3m,
    ft.nr_employed,
    ft.subcribed   

FROM
    fact__tables ft
    LEFT JOIN {{ ref('dim__POUTCOME') }} ip ON ft.poutcome = ip.poutcome
    LEFT JOIN {{ ref('dim__CONTACT') }} ic ON ft.contact = ic.contact
    LEFT JOIN {{ ref('dim__CREDIT') }} icr ON ft.credit = icr.credit
    LEFT JOIN {{ ref('dim__EDUCATION') }} ie ON ft.education = ie.education
    LEFT JOIN {{ ref('dim__JOB') }} ij ON ft.job = ij.job
    LEFT JOIN {{ ref('dim__MARITAL') }} im ON ft.marital = im.marital
    LEFT JOIN {{ ref('dim__LOAN') }} il ON ft.loan = il.loan
    LEFT JOIN {{ ref('dim__HOUSING') }} ih ON ft.housing = ih.housing