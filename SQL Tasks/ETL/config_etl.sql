SHOW DATABASES;
CREATE DATABASE config_db;
USE config_db;

DROP TABLE IF EXISTS cf_etl_table;

CREATE TABLE cf_etl_table
(
    table_id             int PRIMARY KEY,
    schema_name          varchar(20),
    table_name           varchar(20),
    hdfs_upload_location varchar(100),
    hdfs_file_name       varchar(100),
    is_incremental       bool,
    inc_field            varchar(20),
    start_date_time      datetime,
    end_date_time        datetime,
    execution_date       date DEFAULT NULL,
    interval_period      int,
    partition_by         varchar(20)
);

INSERT INTO cf_etl_table(table_id, schema_name, table_name, hdfs_upload_location, hdfs_file_name, is_incremental,
                         inc_field,
                         start_date_time, end_date_time, interval_period, partition_by)
VALUES (1, 'transaction_db', 'transaction', 'hdfs://localhost:19000//mydir/', 'transaction', 1, 'transaction_date',
        '2023-06-23 00:00:00',
        '2023-06-23 23:59:59', 1, 'trans_date_only'),
       (2, 'transaction_db', 'transaction_non_inc', 'hdfs://localhost:19000//mydir/', 'transaction_non_inc', 0, NULL,
        NULL, NULL, NULL, NULL);

SELECT *
FROM cf_etl_table;

CREATE TABLE cf_etl_table_wsl
(
    table_id             int PRIMARY KEY,
    schema_name          varchar(20),
    table_name           varchar(20),
    hdfs_upload_location varchar(100),
    hdfs_file_name       varchar(100),
    is_incremental       bool,
    inc_field            varchar(20),
    start_date_time      datetime,
    end_date_time        datetime,
    execution_date       date DEFAULT NULL,
    interval_period      int,
    partition_by         varchar(20)
);

INSERT INTO cf_etl_table_wsl(table_id, schema_name, table_name, hdfs_upload_location, hdfs_file_name, is_incremental,
                         inc_field,
                         start_date_time, end_date_time, interval_period, partition_by)
VALUES (1, 'transaction_db', 'transaction', 'hdfs://172.24.240.1:19000//mydir/', 'transaction', 1, 'transaction_date',
        '2023-06-23 00:00:00',
        '2023-06-23 23:59:59', 1, 'trans_date_only'),
       (2, 'transaction_db', 'transaction_non_inc', 'hdfs://172.24.240.1:19000//mydir/', 'transaction_non_inc', 0, NULL,
        NULL, NULL, NULL, NULL);

SELECT *
FROM cf_etl_table_wsl;
