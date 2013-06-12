use dse;

insert overwrite table lsagi.lsg_member_cancel
select account_id,
       max(dateint)
from account_membership_status_change_f
where dateint >= 20130601 and
      membership_status = 1 
group by account_id
limit 10;
