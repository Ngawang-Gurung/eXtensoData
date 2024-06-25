SHOW DATABASES;
USE mydb;

SHOW TABLES;

SELECT *
FROM cumsum;

WITH df AS
         (SELECT account_id, month_bs_id, SUM(txn_amt) AS total_txn_amt, SUM(txn_cnt) AS total_txn_cnt
          FROM cumsum
          GROUP BY account_id, month_bs_id
          ORDER BY account_id)
SELECT account_id,
       month_bs_id,
       total_txn_amt, SUM(total_txn_amt) OVER (PARTITION BY account_id ORDER BY month_bs_id) AS cumsum_amt,
       total_txn_cnt, SUM(total_txn_cnt) OVER (PARTITION BY account_id ORDER BY month_bs_id) AS cumsum_cnt
FROM df;

