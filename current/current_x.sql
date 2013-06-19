SELECT COUNT(*)
FROM default.ab_nonmember_events
WHERE other_properties['opType'] = 'ASSOCIATE' AND
      dateint >= 20130617; 
