CREATE DATABASE sequence;
USE sequence;
SHOW TABLES;

SELECT * FROM df;

SELECT DISTINCT(month_id)
FROM df;

with pivot_table as (
SELECT account,
       MAX(CASE WHEN month_id = 31552 THEN 1 ELSE 0 END) AS month_31552,
       MAX(CASE WHEN month_id = 31553 THEN 1 ELSE 0 END) AS month_31553,
       MAX(CASE WHEN month_id = 31554 THEN 1 ELSE 0 END) AS month_31554,
       MAX(CASE WHEN month_id = 31555 THEN 1 ELSE 0 END) AS month_31555,
       MAX(CASE WHEN month_id = 31556 THEN 1 ELSE 0 END) AS month_31556
from df GROUP BY account)
SELECT pivot_table.*, concat(month_31552, month_31553, month_31554, month_31555, month_31556) as sequence
FROM pivot_table;