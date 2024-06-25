USE customer;
SHOW TABLES;

-- Static Pivot Table --

SELECT *
FROM df;

SELECT DISTINCT(month_id)
FROM df;

WITH pivot_table AS (SELECT account,
                            MAX(CASE WHEN month_id = 31552 THEN 1 ELSE 0 END) AS month_31552,
                            MAX(CASE WHEN month_id = 31553 THEN 1 ELSE 0 END) AS month_31553,
                            MAX(CASE WHEN month_id = 31554 THEN 1 ELSE 0 END) AS month_31554,
                            MAX(CASE WHEN month_id = 31555 THEN 1 ELSE 0 END) AS month_31555,
                            MAX(CASE WHEN month_id = 31556 THEN 1 ELSE 0 END) AS month_31556
                     FROM df
                     GROUP BY account)
SELECT pivot_table.*, CONCAT(month_31552,month_31553, month_31554, month_31555, month_31556) AS sequence
FROM pivot_table;

-- Dynamic Pivot Table --

SELECT GROUP_CONCAT(DISTINCT CONCAT(
        ' MAX(CASE WHEN month_id = ', month_id, ' THEN 1 ELSE 0 END ) AS month_', month_id
                             ))
INTO @gc_sql
FROM df;

SET @sql = CONCAT('CREATE TABLE pivot AS SELECT account,', @gc_sql, ' FROM df GROUP BY account;');
SELECT @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Creating Sequence Dynamically

SELECT * FROM pivot;

SELECT GROUP_CONCAT(DISTINCT CONCAT('month_', month_id) ORDER BY month_id)
INTO @distinct_month
FROM df;

SET @sql_query = CONCAT('SELECT pivot.*, CONCAT(', @distinct_month, ') AS sequence FROM pivot');

SELECT @sql_query;
PREPARE stmt FROM @sql_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

