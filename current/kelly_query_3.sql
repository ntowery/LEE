SELECT client_id,
       customer_id,
       allocation_tsp,
       signup_tsp,
       cancel_tsp,
       MAX(is_member_at_allocation) AS is_member_at_allocation,
       MAX(is_ever_member) AS is_ever_member,
       MAX(is_signedup_after_allocation) AS is_signedup_after_allocation,
       cell,
       test_id,
       country_code
FROM 
    (SELECT a.client_id,
            b.customer_id,
            a.allocation_tsp,
            c.signup_tsp,
            c.cancel_tsp,
            CASE WHEN a.allocation_tsp >= c.signup_tsp AND a.allocation_tsp <= c.cancel_tsp THEN
                      'y'
                 ELSE 
                      'n'
            END is_member_at_allocation,
            CASE WHEN a.allocation_tsp >= c.cancel_tsp OR a.allocation_tsp >= c.signup_tsp THEN
                      'y'
                 ELSE
                      'n'
            END is_ever_member,
            CASE WHEN c.signup_tsp > a.allocation_tsp THEN
                      'y'
                 ELSE 
                      'n'
            END is_signedup_after_allocation,
            a.cell,
            a.test_id,
            a.country_code
     FROM 
         (SELECT other_properties['clientId'] AS client_id,
                 CAST(from_unixtime(cast(substr(cast(other_properties['event_utc_ts_ms'] / 1000 AS string), 1, 10) AS bigint), 'yyyyHHddkkmmss') AS BIGINT) AS allocation_tsp,
                 other_properties['cell'] AS cell,
                 other_properties['test'] AS test_id,
                 other_properties['countryCode'] AS country_code
          FROM default.ab_nonmember_events
          WHERE other_properties['clientId'] <> '' AND
                dateint >= 20130612 AND
                other_properties['opType'] = 'ALLOCATE') a
     LEFT OUTER JOIN
         (SELECT DISTINCT    
                 other_properties['clientId'] AS client_id,
                 other_properties['eventValue'] AS customer_id
          FROM default.ab_nonmember_events 
          WHERE other_properties['clientId'] <> '' AND 
                CASE WHEN other_properties['eventKey'] = 'CustomerId' THEN
                          other_properties['eventValue']
                     ELSE
                          ''
                END <> '' AND
                dateint >= 20130612 AND 
                other_properties['opType'] = 'ASSOCIATE') b 
     ON (a.client_id = b.client_id) 
     LEFT OUTER JOIN
        (SELECT account_id,
                CAST(regexp_replace(from_unixtime(unix_timestamp(signup_utc_ts)), ":|-| ", "") AS BIGINT) signup_tsp,
                CASE WHEN cancel_date IS NULL THEN
                          20200101235959
                     ELSE
                         CAST( CONCAT(CAST(cancel_date AS STRING), '235959') AS BIGINT)
                END cancel_tsp
         FROM dse.subscrn_derived_d) c
     ON (b.customer_id = c.account_id)) main
WHERE customer_id IS NOT NULL
GROUP BY client_id, customer_id, allocation_tsp, signup_tsp, cancel_tsp, cell, test_id, country_code
ORDER BY client_id, customer_id, allocation_tsp
LIMIT 100;

--client id, account id (if there is one), allocation date, allocation timestamp, test cell, test id, country id or sk, a flag for whether they are a current member (at the point of allocation...not if they sign-up during the test), a flag for whether they are a former member (at the point of allocation), a flag for any other de-allocation that occurred for reasons besides current/former member (and if you don't have this info, this flag is more a nice to have).
