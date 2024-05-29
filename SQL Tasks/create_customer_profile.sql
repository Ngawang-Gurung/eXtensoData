USE customer;

SHOW TABLES;

SELECT *
FROM customer_profile;

SELECT *
FROM product_category_map;

-- Joining

# CREATE TABLE joined AS
# SELECT td.*, pcm.txn_flow
# FROM rw_transaction_data td
#          JOIN product_category_map pcm
#               ON td.product_id = pcm.product_id AND
#                  td.product_type_id = pcm.product_type_id AND
#                  td.module_id = pcm.module_id;

CREATE TABLE joined AS
SELECT *
FROM rw_transaction_data td
         JOIN product_category_map pcm
              USING (product_id, product_type_id, module_id);

SELECT *
FROM joined;

SELECT COUNT(*)
FROM joined;

-- Creating month column
ALTER TABLE joined
    ADD COLUMN month int;

UPDATE joined
SET month = MONTH(last_modified_date);

SELECT DISTINCT (month)
FROM joined;

-- TXN Flow

SELECT DISTINCT (joined.txn_flow)
FROM joined;

CREATE TABLE txn_flow AS
SELECT payer_account_id,
       SUM(inflow)           AS total_inflow_amount,
       SUM(outflow)          AS total_outflow_amount,
       SUM(valuechain)       AS total_value_chain_amount,
       AVG(inflow)           AS monthly_inflow_amount,
       AVG(outflow)          AS monthly_outlfow_amount,
       AVG(valuechain)       AS monthly_valuechain_amount,
       SUM(inflow_count)     AS total_inflow_count,
       SUM(outflow_count)    AS total_outflow_count,
       SUM(valuechain_count) AS total_value_chain_count,
       AVG(inflow_count)     AS monthly_inflow_count,
       AVG(outflow_count)    AS monthly_outlfow_count,
       AVG(valuechain_count) AS monthly_valuechain_count
FROM (SELECT
          -- Same as creating Pivot Table where index = [payer_account_id, month], columns = txn_flow and value = amount
          j.payer_account_id,
          SUM(IF(j.txn_flow = 'Inflow', j.amount, 0))           AS inflow,
          SUM(IF(j.txn_flow = 'OutFlow', j.amount, 0))          AS outflow,
          SUM(IF(j.txn_flow = 'Value Chain', j.amount, 0))      AS valuechain,
          COUNT(IF(j.txn_flow = 'Inflow', j.amount, NULL))      AS inflow_count,
          COUNT(IF(j.txn_flow = 'OutFlow', j.amount, NULL))     AS outflow_count,
          COUNT(IF(j.txn_flow = 'Value Chain', j.amount, NULL)) AS valuechain_count
      FROM joined j
      GROUP BY j.payer_account_id, j.month) pivot_monthly
GROUP BY payer_account_id;

-- Latest used product and last transaction date

ALTER TABLE joined
    ADD COLUMN dates datetime;

UPDATE joined
SET dates = CAST(last_modified_date AS DATETIME) + CAST(time AS TIME);

SELECT joined.last_modified_date, joined.time, joined.dates
FROM joined;

CREATE TABLE latest_txn AS
SELECT RN.payer_account_id, RN.product_name as latest_used_product, RN.dates as latest_transaction_date
FROM (SELECT joined.payer_account_id,
             joined.product_name,
             joined.dates,
             ROW_NUMBER() OVER (PARTITION BY joined.payer_account_id ORDER BY joined.dates DESC) AS RN
      FROM joined) AS RN
WHERE RN = 1;

-- Reward Points
CREATE TABLE reward_points AS
SELECT payer_account_id, SUM(reward_point) as reward_points
FROM joined
GROUP BY payer_account_id;

-- Revenue Amount
CREATE TABLE revenue_amount AS
SELECT payer_account_id, SUM(revenue_amt) AS total_revenue, AVG(revenue_amt) AS monthly_revenue
FROM (SELECT joined.payer_account_id,
             SUM(joined.revenue_amount) AS revenue_amt
      FROM joined
      GROUP BY joined.payer_account_id, joined.month) revenue_pivot
GROUP BY payer_account_id;

# Product Usage
CREATE TABLE product_usage AS
SELECT payer_account_id, COUNT(product_name) AS usage_count
FROM joined
GROUP BY payer_account_id;

-- Nth Used Product
CREATE TABLE nth_used_prod AS
WITH product_usage AS (SELECT payer_account_id, product_name, COUNT(product_name) AS usage_count
                       FROM joined
                       GROUP BY payer_account_id, product_name),
     ranked_products AS (SELECT payer_account_id,
                                product_name,
                                usage_count,
                                ROW_NUMBER() OVER (PARTITION BY payer_account_id ORDER BY usage_count DESC) AS row_num
                         FROM product_usage)
SELECT payer_account_id,
       MAX(CASE WHEN row_num = 1 THEN product_name END) AS most_used_product,
       MAX(CASE WHEN row_num = 2 THEN product_name END) AS second_most_used_product,
       MAX(CASE WHEN row_num = 3 THEN product_name END) AS third_most_used_product
FROM ranked_products
GROUP BY payer_account_id;

SHOW TABLES;

CREATE TABLE new_customer_profile
SELECT *
FROM txn_flow
         JOIN latest_txn USING (payer_account_id)
         JOIN revenue_amount USING (payer_account_id)
         JOIN reward_points USING (payer_account_id)
         JOIN product_usage USING (payer_account_id)
         JOIN nth_used_prod USING (payer_account_id);

SELECT * FROM new_customer_profile;



















