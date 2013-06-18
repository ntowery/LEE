SELECT *
FROM default.ab_nonmember_events
WHERE other_properties['clientId'] <> '' AND
      dateint >= 20130612 AND
      other_properties['opType'] = 'ALLOCATE' AND 
      other_properties['clientId'] = 'TkZQUzMtMDAxLTdENFdGVEFLVTBLRjNFMVVVOEFQSzJZODYw';
