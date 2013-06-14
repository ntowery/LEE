use default;

SELECT a.client_id,
       b.account_id,
       a.allocation_date,
       a.tsp,
       a.cell,
       a.test_id,
       a.country_code
FROM 
    (SELECT other_properties['clientId'] AS client_id,
            other_properties['accountOwnerId'] AS account_id,
            dateint AS allocation_date,
            from_unixtime(cast(substr(cast(event_utc_ms AS string), 1, 10) AS bigint), 'yyyyHHddkkmmss') AS tsp,
            other_properties['cell'] AS cell,
            other_properties['test'] AS test_id,
            other_properties['countryCode'] AS country_code
     FROM default.ab_nonmember_events
     WHERE other_properties['clientId'] <> '' AND
           dateint>=20130612 AND
           other_properties['opType'] = 'ALLOCATE') a
LEFT OUTER JOIN
    (SELECT DISTINCT    
            other_properties['clientId'] AS client_id,
            other_properties['accountOwnerId'] AS account_id,
            dateint AS association_date
     FROM default.ab_nonmember_events 
     WHERE other_properties['clientId'] <> '' AND 
           other_properties['accountOwnerId'] <> '' AND
           dateint>=20130612 AND 
           other_properties['opType'] = 'ASSOCIATE') b 
ON (a.client_id = b.client_id)
WHERE b.association_date >= a.allocation_date
LIMIT 10;

--client id, account id (if there is one), allocation date, allocation timestamp, test cell, test id, country id or sk, a flag for whether they are a current member (at the point of allocation...not if they sign-up during the test), a flag for whether they are a former member (at the point of allocation), a flag for any other de-allocation that occurred for reasons besides current/former member (and if you don't have this info, this flag is more a nice to have).
