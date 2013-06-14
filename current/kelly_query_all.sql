use default;

SELECT *
FROM default.ab_nonmember_events 
WHERE dateint>=20130612 AND
      other_properties['opType'] = 'ASSOCIATE' AND
      other_properties['clientId'] <> '' 
LIMIT 1;

--client id, account id (if there is one), allocation date, allocation timestamp, test cell, test id, country id or sk, a flag for whether they are a current member (at the point of allocation...not if they sign-up during the test), a flag for whether they are a former member (at the point of allocation), a flag for any other de-allocation that occurred for reasons besides current/former member (and if you don't have this info, this flag is more a nice to have).
