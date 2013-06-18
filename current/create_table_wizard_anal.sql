USE lsagi;

CREATE EXTERNAL TABLE lsagi.wizard_anal
(client_id STRING,
 customer_id STRING,
 allocation_tsp STRING,
 is_member_at_allocation STRING,
 is_former_member STRING,
 is_signedup_after_allocation STRING,
 is_signed_in_28_days STRING,
 cell STRING,
 test_id STRING,
 country_code STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
location 's3n://netflix-dataoven-prod-users/lsagi/wizard_anal';
