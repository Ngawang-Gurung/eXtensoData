show DATABASES;

use client_rw;

show tables;

select * from fc_transaction_base;

CREATE TABLE salary_transactions AS
SELECT *
FROM fc_transaction_base
WHERE description1 LIKE '%salary%';

select * from salary_transactions;

	