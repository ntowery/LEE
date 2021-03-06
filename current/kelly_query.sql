use default;

SELECT other_properties['clientId'] AS client_id,
       dateint AS allocation_date,
       from_unixtime(cast(substr(cast(event_utc_ms AS string), 1, 10) AS bigint), 'yyyyHHddkkmmss') AS tsp,  
       other_properties['cell'] AS cell,
       other_properties['test'] AS test_id,
       other_properties['countryCode'] AS country_code,
       CASE WHEN other_properties['eventKey'] = 'CustomerId' THEN 
                 other_properties['eventValue'] 
       END customer_id
FROM default.ab_nonmember_events 
WHERE other_properties['test'] = '' AND 
      dateint >= 20130612 AND 
      other_properties['opType'] = 'ASSOCIATE' 
LIMIT 10;

--client id, account id (if there is one), allocation date, allocation timestamp, test cell, test id, country id or sk, a flag for whether they are a current member (at the point of allocation...not if they sign-up during the test), a flag for whether they are a former member (at the point of allocation), a flag for any other de-allocation that occurred for reasons besides current/former member (and if you don't have this info, this flag is more a nice to have).
