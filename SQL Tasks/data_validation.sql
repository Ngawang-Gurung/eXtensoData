USE quality;
SHOW TABLES;

SELECT *
FROM dev_data;
SELECT *
FROM qa_data;

DROP TABLE IF EXISTS validator;
CREATE TABLE validator AS
SELECT dev_data.account AS dev_account,
       dev_data.amount  AS dev_amount,
       qa_data.account  AS qa_account,
       qa_data.amount   AS qa_amount
FROM dev_data
LEFT JOIN qa_data ON dev_data.account = qa_data.account
UNION
SELECT dev_data.account AS dev_account,
       dev_data.amount  AS dev_amount,
       qa_data.account  AS qa_account,
       qa_data.amount   AS qa_amount
FROM dev_data
RIGHT JOIN qa_data ON dev_data.account = qa_data.account;

ALTER TABLE validator ADD COLUMN status varchar(50);

UPDATE validator
SET status = CASE WHEN dev_amount = qa_amount THEN 'Pass' ELSE 'Fail' END;

SELECT * FROM validator;
