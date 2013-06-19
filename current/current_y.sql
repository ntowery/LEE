SELECT COUNT(*)
FROM default.ab_nonmember_events
WHERE other_properties['opType'] = 'ASSOCIATE' AND
      dateint >= 20130617 AND
      other_properties['clientId'] <> '' AND
      CASE WHEN other_properties['eventKey'] = 'CustomerId' THEN
                other_properties['eventValue']
           ELSE
                ''
     END <> '';
