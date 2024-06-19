
-- Replace NULL values with 0--

SHOW DATABASES;

USE client_rw;

SHOW TABLES;

CREATE TABLE facts_dummy AS
SELECT *
FROM facts;

SELECT *
FROM facts_dummy;

UPDATE facts_dummy
SET average_deposit = 0
WHERE average_deposit IS NULL;


-- Find count of NULL values in a col umn --
USE customer;

SELECT COUNT(*) - COUNT(third_most_used_product) AS third_most_used_product_null_count
FROM new_customer_profile;

SELECT COUNT(*) AS third_most_used_product_null_count
FROM new_customer_profile
WHERE new_customer_profile.third_most_used_product IS NULL;

SELECT SUM(IF(new_customer_profile.third_most_used_product IS NULL, 1, 0)) AS third_most_used_product_null_count
FROM new_customer_profile;


