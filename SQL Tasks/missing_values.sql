SHOW DATABASES;

USE fc_facts;

SHOW TABLES;

CREATE TABLE facts_dummy AS
SELECT *
FROM facts;

SELECT *
FROM facts_dummy;

UPDATE facts_dummy
SET average_deposit = 0
WHERE average_deposit IS NULL;





