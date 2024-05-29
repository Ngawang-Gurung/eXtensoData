USE customer;

SHOW TABLES;

SELECT *
FROM customer_profile;

SELECT *
FROM product_category_map;

SELECT *
FROM rw_transaction_data td
JOIN product_category_map pcm
    USING (product_id, product_type_id, module_id);

# Joining

CREATE TABLE joined AS
SELECT td.*, pcm.txn_flow
FROM rw_transaction_data td
         JOIN product_category_map pcm
              ON td.product_id = pcm.product_id AND
                 td.product_type_id = pcm.product_type_id AND
                 td.module_id = pcm.module_id;

DROP TABLE joined;

SELECT *
FROM joined;

SELECT COUNT(*)
FROM joined;

# Creating month column
ALTER TABLE joined
    ADD COLUMN month int;

UPDATE joined
SET month = MONTH(last_modified_date);

SELECT DISTINCT (month)
FROM joined;

# Reward Points
SELECT payer_account_id, SUM(reward_point)
FROM joined
GROUP BY payer_account_id;

# Product Usage
SELECT payer_account_id, COUNT(joined.product_id)
FROM joined
GROUP BY payer_account_id;

-- TXN Flow

SELECT DISTINCT (joined.txn_flow)
FROM joined;

CREATE TABLE pivot_monthly AS
SELECT joined.payer_account_id, joined.month,
       SUM(IF(joined.txn_flow = 'Inflow', joined.amount, 0))        AS inflow,
       SUM(IF(joined.txn_flow = 'OutFlow', joined.amount, 0))       AS outflow,
       SUM(IF(joined.txn_flow = 'Value Chain', joined.amount, 0))   AS valuechain,
       COUNT(IF(joined.txn_flow = 'Inflow', joined.amount, NULL))      AS inflow_count,
       COUNT(IF(joined.txn_flow = 'OutFlow', joined.amount, NULL))     AS outflow_count,
       COUNT(IF(joined.txn_flow = 'Value Chain', joined.amount, NULL)) AS valuechain_count
FROM joined
GROUP BY joined.payer_account_id, joined.month;

SELECT * FROM pivot_monthly;
DROP TABLE pivot_monthly;

SELECT pivot_monthly.payer_account_id,
       SUM(pivot_monthly.inflow) AS total_inflow_amount,
       SUM(pivot_monthly.outflow) AS total_outflow_amount,
       SUM(pivot_monthly.valuechain) AS total_value_chain_amount,
       AVG(pivot_monthly.inflow) AS monthly_inflow_amount,
       AVG(pivot_monthly.outflow) AS monthly_outlfow_amount,
       AVG(pivot_monthly.valuechain) AS monthly_valuechain_amount,
       SUM(pivot_monthly.inflow_count) AS total_inflow_count,
       SUM(pivot_monthly.outflow_count) AS total_outflow_count,
       SUM(pivot_monthly.valuechain_count) AS total_value_chain_count,
       AVG(pivot_monthly.inflow_count) AS monthly_inflow_count,
       AVG(pivot_monthly.outflow_count) AS monthly_outlfow_count,
       AVG(pivot_monthly.valuechain_count) AS monthly_valuechain_count
FROM pivot_monthly
GROUP BY pivot_monthly.payer_account_id;











