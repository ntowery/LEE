use default;

SELECT other_properties['clientId'] AS client_id,
       other_properties['accountOwnerId'] AS account_id,
       dateint AS allocation_date,
       from_unixtime(cast(substr(cast(event_utc_ms as string), 1, 10) as bigint), '%X%m%d%H%i%s') ,  
       other_properties['cell'] AS cell,
       other_properties['test'] AS test_id,
       other_properties['countryCode'] AS country_code
FROM default.ab_nonmember_events 
WHERE dateint>=20130610 AND 
      other_properties['opType'] = 'ALLOCATE' 
LIMIT 1;

--client id, account id (if there is one), allocation date, allocation timestamp, test cell, test id, country id or sk, a flag for whether they are a current member (at the point of allocation...not if they sign-up during the test), a flag for whether they are a former member (at the point of allocation), a flag for any other de-allocation that occurred for reasons besides current/former member (and if you don't have this info, this flag is more a nice to have).
