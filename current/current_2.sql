SELECT CAST(FROM_UNIXTIME(CAST(event_utc_ms / 1000 AS BIGINT), 'yyyyMMddkkmmss') AS BIGINT),
       from_unixtime(cast(substr(cast(event_utc_ms as string), 1, 10) as bigint)),
       event_utc_ms,
       CAST(event_utc_ms / 1000 AS BIGINT)
FROM default.ab_nonmember_events
WHERE other_properties['clientId'] <> '' AND
      dateint >= 20130612 AND
      other_properties['opType'] = 'ALLOCATE' AND 
      other_properties['clientId'] = 'TkZQUzMtMDAxLThFUENUSkROTVYyMVdNQ1FZTVY0S1BGVVI4';
