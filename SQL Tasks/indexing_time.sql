show databases;
use client_rw;
show tables;

-- Before indexing fetch takes 0.016 sec
select * from fc_transaction_base where lcy_amount = 20000;

-- After indexing fetch takes 0.00 sec
create index tb on fc_transaction_base (lcy_amount);

select * from fc_transaction_base where lcy_amount = 20000;

DROP INDEX tb ON fc_transaction_base;