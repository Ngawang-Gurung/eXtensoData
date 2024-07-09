SHOW DATABASES;
USE client_rw;
SHOW TABLES;

-- Average and Weighted Average on Deposit and Withdraw Column of fc_transaction_base --

-- Average --
DROP TEMPORARY TABLE IF EXISTS average;
CREATE TEMPORARY TABLE average AS
WITH monthly_facts AS (SELECT MONTH(tb.tran_date)  AS month,
                              tb.account_number,
                              AVG(CASE WHEN dc_indicator = 'deposit' AND is_salary = 1 THEN lcy_amount ELSE 0 END) AS income_deposit,
                              AVG(CASE WHEN dc_indicator = 'deposit' AND is_salary = 0 THEN lcy_amount ELSE 0 END) AS other_deposit,
                              AVG(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE 0 END) AS withdraw
                       FROM fc_transaction_base AS tb
                       GROUP BY tb.account_number, month)
SELECT monthly_facts.account_number,
       AVG(income_deposit) AS avg_income_deposit,
       AVG(other_deposit)  AS avg_other_deposit,
       AVG(withdraw)       AS avg_withdraw
FROM monthly_facts
GROUP BY monthly_facts.account_number
ORDER BY monthly_facts.account_number;

-- Weighted Average --
DROP TEMPORARY TABLE IF EXISTS cross_join_distinct_acc_num_mth ;
CREATE TEMPORARY TABLE cross_join_distinct_acc_num_mth as
WITH distinct_acc_num_mth AS (
SELECT DISTINCT account_number FROM fc_transaction_base), distinct_month AS (
    SELECT DISTINCT (MONTH(fc_transaction_base.tran_date)) AS mth FROM fc_transaction_base
    )
SELECT *
FROM distinct_acc_num_mth
         CROSS JOIN distinct_month
ORDER BY account_number, mth;

DROP TEMPORARY TABLE IF EXISTS full_table;
CREATE TEMPORARY TABLE full_table AS
WITH tb AS (
    SELECT *, MONTH(tran_date) AS mth
    FROM fc_transaction_base
)
SELECT *
FROM cross_join_distinct_acc_num_mth
LEFT JOIN tb USING (account_number, mth);

DROP TEMPORARY TABLE IF EXISTS weighted_average;
CREATE TEMPORARY TABLE weighted_average AS
WITH monthly_weighted_facts AS  (
SELECT ft.account_number,
       mth,
       ROW_NUMBER() OVER (PARTITION BY account_number ORDER BY mth)     AS weight,
       AVG(CASE WHEN dc_indicator = 'deposit' AND is_salary = 1 THEN lcy_amount ELSE 0 END) AS income_deposit,
       AVG(CASE WHEN dc_indicator = 'deposit' AND is_salary = 0 THEN lcy_amount ELSE 0 END) AS other_deposit,
       AVG(CASE WHEN dc_indicator = 'withdraw' THEN lcy_amount ELSE 0 END)                  AS withdraw
FROM full_table AS ft
GROUP BY ft.account_number, mth)
SELECT monthly_weighted_facts.account_number,
       ROUND(SUM(income_deposit * weight) / SUM(weight), 2) AS weighted_avg_income_deposit,
       ROUND(SUM(other_deposit * weight) / SUM(weight), 2)  AS weighted_avg_other_deposit,
       ROUND(SUM(withdraw * weight) / SUM(weight), 2)       AS weighted_avg_withdraw
FROM monthly_weighted_facts
GROUP BY monthly_weighted_facts.account_number;

SELECT *
FROM average
         NATURAL JOIN weighted_average;

-- Average and Weighted Average on Balance Column of transaction_summary --

-- Average Balance --
DROP TEMPORARY TABLE IF EXISTS average_balance;
CREATE TEMPORARY TABLE average_balance AS
WITH monthly_facts AS (
SELECT ts.Account_No,
       MONTH(ts.Date)   AS mth,
       AVG(Balance_Amt) AS balance_amt
FROM transaction_summary AS ts
GROUP BY ts.Account_No, mth ORDER BY Account_No, mth)
SELECT Account_No, AVG(balance_amt) as avg_balance FROM monthly_facts
GROUP BY Account_No;

-- Average Weighted Balance --
DROP TEMPORARY TABLE IF EXISTS cross_join_distinct_acc_num_mth_balance ;
CREATE TEMPORARY TABLE cross_join_distinct_acc_num_mth_balance as
WITH distinct_acc_num_mth AS (
SELECT DISTINCT Account_No FROM transaction_summary), distinct_month AS (
    SELECT DISTINCT (MONTH (transaction_summary.Date)) AS mth FROM transaction_summary
    )
SELECT *
FROM distinct_acc_num_mth
         CROSS JOIN distinct_month
ORDER BY Account_No, mth;

DROP TEMPORARY TABLE IF EXISTS full_table_balance;
CREATE TEMPORARY TABLE full_table_balance AS
WITH tb AS (
    SELECT *, MONTH(Date) AS mth
    FROM transaction_summary
)
SELECT *
FROM cross_join_distinct_acc_num_mth_balance
LEFT JOIN tb USING (Account_No, mth);

DROP TEMPORARY TABLE IF EXISTS weighted_average_balance;
CREATE TEMPORARY TABLE weighted_average_balance AS
WITH weighted_balance AS
         (SELECT ftb.Account_No,
                 mth,
                 ROW_NUMBER() OVER (PARTITION BY Account_No ORDER BY mth) AS weight,
                 AVG(Balance_Amt)                                         AS balance_amt
          FROM full_table_balance AS ftb
          GROUP BY ftb.Account_No, mth),
     weighted_prev_balance AS
         (SELECT weighted_balance.*,
                 LAG(balance_amt, 1, 0) OVER (PARTITION BY Account_No ORDER BY mth) AS prev_balance
          FROM weighted_balance),
     monthly_weighted_facts AS
         (SELECT weighted_prev_balance.*,
                 (CASE WHEN balance_amt <> 0 THEN balance_amt ELSE prev_balance END) AS final_balance
          FROM weighted_prev_balance)
SELECT monthly_weighted_facts.Account_No,
       ROUND(SUM(balance_amt * weight) / SUM(weight), 2) AS weighted_avg_balance
FROM monthly_weighted_facts
GROUP BY monthly_weighted_facts.Account_No;

SELECT *
FROM average_balance
         NATURAL JOIN weighted_average_balance;



