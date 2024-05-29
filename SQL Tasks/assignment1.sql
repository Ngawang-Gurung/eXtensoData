SHOW databases;
CREATE database client_rw;
USE client_rw;

-- TASK 1
SHOW TABLES;

DESCRIBE fc_account_master;
DESCRIBE fc_transaction_base;

SELECT *
FROM fc_account_master
LIMIT 2;

SELECT *
FROM fc_transaction_base;

ALTER TABLE fc_transaction_base RENAME COLUMN ï»¿tran_date TO tran_date;

-- TASK 2 

SELECT account_number,
       MONTH(tran_date)                                                            AS monthly,
       AVG(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)       AS average_deposit,
       AVG(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END)      AS average_withdraw,
       STD(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)       AS std_deposit,
       STD(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END)      AS std_withdraw,
       VARIANCE(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)  AS var_deposit,
       VARIANCE(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END) AS var_withdraw
FROM fc_transaction_base
GROUP BY account_number, monthly;

SELECT *
FROM fc_account_master AS am
         JOIN fc_transaction_base AS tb
              ON am.account_number = tb.account_number;

-- TASK 2/3

CREATE TABLE facts AS
SELECT am.account_number,
       am.customer_code,
       MONTH(tran_date)                                                            AS monthly,
       AVG(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)       AS average_deposit,
       AVG(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END)      AS average_withdraw,
       STD(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)       AS std_deposit,
       STD(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END)      AS std_withdraw,
       VARIANCE(CASE WHEN dc_indicator = 'deposit' THEN lcy_amount ELSE NULL END)  AS var_deposit,
       VARIANCE(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE NULL END) AS var_withdraw
FROM fc_account_master AS am
         JOIN fc_transaction_base AS tb
              ON am.account_number = tb.account_number
GROUP BY am.account_number, monthly, am.customer_code;


-- Dump facts table into fc_facts database
CREATE DATABASE fc_facts;

USE fc_facts;

SHOW TABLES;

SELECT *
FROM facts
LIMIT 10;


