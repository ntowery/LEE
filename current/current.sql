SELECT account_id,
       CAST(regexp_replace(from_unixtime(unix_timestamp(signup_utc_ts)), ":|-| ", "") AS BIGINT) signup_utc_ts,
       signup_date,
       CASE WHEN cancel_date IS NULL THEN 
                 20200101000000
            ELSE
                CAST( CONCAT(CAST(cancel_date AS STRING), '000000') AS BIGINT)
       END cancel_date
FROM dse.subscrn_derived_d 
WHERE account_id = 20675816
ORDER BY 2 ASC 
LIMIT 1000000;

