use dse;

create table lsagi.lsg_member_status_1 as 
select account_id, 
       max(dateint) as dateint
from dse.account_membership_status_change_f
where dateint >= 20130601
group by account_id;
