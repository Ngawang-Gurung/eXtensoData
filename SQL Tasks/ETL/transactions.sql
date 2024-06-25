SHOW DATABASES;
CREATE DATABASE transaction_db;
USE transaction_db;

DROP TABLE IF EXISTS transaction;
CREATE TABLE transaction
(
    txn_id   int AUTO_INCREMENT PRIMARY KEY,
    txn_date datetime,
    acc_id   varchar(25),
    product  varchar(50),
    status   bool
);

DESCRIBE transaction;

INSERT INTO transaction (txn_date, acc_id, product, status)
VALUES ('2023-06-23 10:30:00', 'ACC001', 'ProductA', 0),
       ('2023-06-24 11:00:00', 'ACC002', 'ProductB', 0),
       ('2023-06-25 09:45:00', 'ACC003', 'ProductC', 0),
       ('2023-06-26 12:15:00', 'ACC004', 'ProductD', 0),
       ('2023-06-27 14:30:00', 'ACC005', 'ProductE', 0),
       ('2023-06-28 08:20:00', 'ACC006', 'ProductF', 0),
       ('2023-06-29 16:50:00', 'ACC007', 'ProductG', 0),
       ('2023-06-30 13:10:00', 'ACC008', 'ProductH', 0);

SELECT *
FROM transaction;