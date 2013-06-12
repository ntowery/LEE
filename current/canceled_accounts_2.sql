use dse;

select *
from account_membership_status_change_f a join lsagi.lsg_member_cancel b
on a.account_id = b.account_id
where a.dateint >= 19900101
order by a.account_id asc
limit 100000;
