use lsagi;

create table lsagi.lsg_member_cancel 
(account_id bigint,
 dateint bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
location 's3n://netflix-dataoven-prod-users/lsagi/dummy';
