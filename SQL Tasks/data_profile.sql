USE customer;

SHOW TABLES;

-- Find count of NULL values in a column --

SELECT COUNT(*) - COUNT(third_most_used_product) AS third_most_used_product_null_count
FROM new_customer_profile;

SELECT COUNT(*) AS third_most_used_product_null_count
FROM new_customer_profile
WHERE new_customer_profile.third_most_used_product IS NULL;

SELECT SUM(IF(new_customer_profile.third_most_used_product IS NULL, 1, 0)) AS third_most_used_product_null_count
FROM new_customer_profile;

-- Using stored procedure to describe table --

DELIMITER //
DROP PROCEDURE IF EXISTS describe_column //
CREATE PROCEDURE describe_column(IN columnName varchar(255))
BEGIN
    SET @sql_query = CONCAT('SELECT AVG(', columnName, ') avg, MIN(', columnName, ') min, MAX(', columnName, ') max, SUM(', columnName, ') sum, COUNT(', columnName, ') count,
                        ( SELECT COUNT(*) FROM new_customer_profile WHERE ', columnName, ' IS NULL) null_count
                        FROM new_customer_profile');
    PREPARE statement FROM @sql_query;
    EXECUTE statement;
    DEALLOCATE PREPARE statement;
END //

DELIMITER ;

SELECT * FROM new_customer_profile;
CALL describe_column('total_inflow_amount');

-- Find all column names of a table in database

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'new_customer_profile'
  AND table_schema = 'customer';

-- Dynamic SQL --





