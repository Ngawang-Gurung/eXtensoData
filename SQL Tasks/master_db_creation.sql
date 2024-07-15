USE client_rw;

SHOW TABLES;

DROP TABLE IF EXISTS md_account_master;
CREATE TABLE rw_account_master
(
    account_number  VARCHAR(255) PRIMARY KEY,
    mobile_number   INT,
    acc_open_date   DATE,
    acc_closed_date DATE,
    address         VARCHAR(255)
);

CREATE TABLE md_account_master LIKE rw_account_master;
CREATE TABLE md_account_master_archive LIKE rw_account_master;

ALTER TABLE md_account_master
    ADD COLUMN modified_date DATETIME DEFAULT NOW();
ALTER TABLE md_account_master_archive
    ADD COLUMN created_date DATETIME DEFAULT NOW();
ALTER TABLE md_account_master_archive
    MODIFY COLUMN account_number VARCHAR(255);

TRUNCATE TABLE rw_account_master;
TRUNCATE TABLE md_account_master;

INSERT INTO rw_account_master
VALUES ('123ABC', 9840, '2024-01-01', '2024-12-31', 'X');
INSERT INTO rw_account_master
VALUES ('123PQR', 9841, '2024-01-01', '2024-12-31', 'Y');
INSERT INTO rw_account_master
VALUES ('123XYZ', 9842, '2024-01-01', '2024-12-31', 'Z');

UPDATE rw_account_master
SET mobile_number = 9851
WHERE account_number = '123PQR';

INSERT INTO md_account_master VALUES ('123ABC', 9840, '2024-01-01', '2024-12-31', 'X', NULL);
INSERT INTO md_account_master VALUES ('123PQR', 9841, '2024-01-01', '2024-12-31', 'Y', NULL);

SELECT *
FROM rw_account_master;
SELECT *
FROM md_account_master;

SHOW TABLE STATUS WHERE Name = 'rw_account_master';



