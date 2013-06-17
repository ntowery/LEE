use default;

SELECT COUNT(*)
FROM default.ab_nonmember_events
WHERE dateint >= 20130612 AND
      other_properties['opType'] = 'ALLOCATE'; 
