{{ 
  config(
    materialized = 'view'
  ) 
}}

SELECT 
    ic.contact AS Contact_Type,
    ft.month AS Contact_Month,
    ft.day_of_week AS Contact_Day_of_Week,
    ft.campaign,
    ft.pdays AS Days_Since_Last_Contact,
    ip.poutcome AS Previous_Campaign_Outcome,
    ft.subcribed
FROM {{ ref('stg__marketing') }} ft
LEFT JOIN {{ ref('dim__CONTACT') }} ic ON ft.contact = ic.contact
LEFT JOIN {{ ref('dim__POUTCOME') }} ip ON ft.poutcome = ip.poutcome
GROUP BY Contact_Type, Contact_Month, Contact_Day_of_Week, ft.campaign, Days_Since_Last_Contact, Previous_Campaign_Outcome, ft.subcribed
ORDER BY ft.subcribed DESC