SHOW DATABASES;

USE client_rw;

SHOW TABLES;

SELECT *
FROM fc_transaction_base;

CREATE TABLE salary_transactions AS
SELECT *
FROM fc_transaction_base
WHERE description1 LIKE '%salary%';

SELECT *
FROM salary_transactions;

	