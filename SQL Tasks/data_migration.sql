SHOW DATABASES;
CREATE DATABASE migration;
USE migration;

DROP TABLE IF EXISTS transaction;
CREATE TABLE transaction
(
    tnx_id   int AUTO_INCREMENT PRIMARY KEY,
    tnx_date datetime,
    acc_id   varchar(25),
    product  varchar(50),
    status   bool
);

DESCRIBE transaction;

INSERT INTO transaction (tnx_date, acc_id, product, status)
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


DROP TABLE IF EXISTS cf_etl_table;

CREATE TABLE cf_etl_table
(
    id              int AUTO_INCREMENT PRIMARY KEY,
    schema_name    varchar(20),
    table_name     varchar(20),
    hdfs_upload_location varchar(100),
    hdfs_file_name  varchar(100),
    start_date_time datetime,
    end_date_time   datetime,
    is_incremental  bool,
    execution_date  date DEFAULT NULL,
    inc_field       varchar(20)
);

INSERT INTO cf_etl_table(schema_name, table_name, hdfs_upload_location, hdfs_file_name, start_date_time,
                         end_date_time, is_incremental, inc_field)
VALUES ('migration', 'transaction', 'hdfs://localhost:19000//mydir/', 'transaction', '2023-06-23 00:00:00',
        '2023-06-23 23:59:59', 1, 'tnx_date');

SELECT *
FROM cf_etl_table;

