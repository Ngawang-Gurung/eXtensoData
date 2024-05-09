SHOW databases;
CREATE database client_rw;
USE client_rw;

-- TASK 1
show tables;

describe fc_account_master;
describe fc_transaction_base;

select * from fc_account_master limit 2;
select * from fc_transaction_base;

ALTER TABLE fc_transaction_base RENAME COLUMN ï»¿tran_date to tran_date;

-- TASK 2 

select 
account_number, 
month(tran_date) as monthly,
avg(case when dc_indicator = 'deposit' then lcy_amount else null end) as average_deposit,
avg(case when dc_indicator = 'withdraw' then lcy_amount else null end) as average_withdraw,
std(case when dc_indicator = 'deposit' then lcy_amount else null end) as std_deposit,
std(case when dc_indicator = 'withdraw' then lcy_amount else null end) as std_withdraw,
variance(case when dc_indicator = 'deposit' then lcy_amount else null end) as var_deposit,
variance(case when dc_indicator = 'withdraw' then lcy_amount else null end) as var_withdraw
from fc_transaction_base 
group by account_number, monthly;

select * from fc_account_master as am 
join fc_transaction_base as tb 
on am.account_number = tb.account_number;

-- TASK 3

select 
am.account_number, am.customer_code,
month(tran_date) as monthly,
avg(case when dc_indicator = 'deposit' then lcy_amount else null end) as average_deposit,
avg(case when dc_indicator = 'withdraw' then lcy_amount else null end) as average_withdraw,
std(case when dc_indicator = 'deposit' then lcy_amount else null end) as std_deposit,
std(case when dc_indicator = 'withdraw' then lcy_amount else null end) as std_withdraw,
variance(case when dc_indicator = 'deposit' then lcy_amount else null end) as var_deposit,
variance(case when dc_indicator = 'withdraw' then lcy_amount else null end) as var_withdraw
from fc_account_master as am 
join fc_transaction_base as tb 
on am.account_number = tb.account_number
group by am.account_number, monthly, am.customer_code;

create table facts as 
	select 
	am.account_number, am.customer_code,
	month(tran_date) as monthly,
	avg(case when dc_indicator = 'deposit' then lcy_amount else null end) as average_deposit,
	avg(case when dc_indicator = 'withdraw' then lcy_amount else null end) as average_withdraw,
	std(case when dc_indicator = 'deposit' then lcy_amount else null end) as std_deposit,
	std(case when dc_indicator = 'withdraw' then lcy_amount else null end) as std_withdraw,
	variance(case when dc_indicator = 'deposit' then lcy_amount else null end) as var_deposit,
	variance(case when dc_indicator = 'withdraw' then lcy_amount else null end) as var_withdraw
	from fc_account_master as am 
	join fc_transaction_base as tb 
	on am.account_number = tb.account_number
	group by am.account_number, monthly, am.customer_code; 
    
create database fc_facts;
use fc_facts;
show tables;
select * from facts limit 10;


