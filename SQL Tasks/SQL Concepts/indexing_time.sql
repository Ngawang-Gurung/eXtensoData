SHOW DATABASES;
USE client_rw;
SHOW TABLES;

-- Before indexing fetch takes 0.016 sec
SELECT *
FROM fc_transaction_base
WHERE lcy_amount = 20000 LIMIT 0, 1234567;

-- After indexing fetch takes 0.00 sec
CREATE INDEX lc_amount_idx ON fc_transaction_base (lcy_amount);

SELECT *
FROM fc_transaction_base
WHERE lcy_amount = 20000 LIMIT 0, 1234567;

DROP INDEX lc_amount_idx ON fc_transaction_base;