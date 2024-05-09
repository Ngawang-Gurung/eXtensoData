show databases;

use fc_facts;

show tables;

create table facts_dummy as
select * from facts;

select * from facts_dummy; 

UPDATE facts_dummy 
SET average_deposit = 0 
WHERE average_deposit IS NULL;





