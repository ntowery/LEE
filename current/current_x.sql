SELECT COUNT(*)
FROM default.ab_nonmember_events
WHERE other_properties['opType'] = 'ASSOCIATE' AND
      other_properties['test'] = 3796 AND
      dateint >= 20130617; 
