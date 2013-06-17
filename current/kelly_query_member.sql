use dse;

SELECT account_id,
       signup_date,
       cancel_date,
     
FROM dse.subscrn_derived_d
WHERE is_latest_derived_subscrn = 1 AND
      
