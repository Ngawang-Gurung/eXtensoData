SHOW DATABASES;
CREATE DATABASE config_db;
USE config_db;

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
VALUES ('transaction_db', 'transaction', 'hdfs://localhost:19000//mydir/', 'transaction', '2023-06-23 00:00:00',
        '2023-06-23 23:59:59', 1, 'tnx_date');

SELECT *
FROM cf_etl_table;

-- For WSL --

DROP TABLE IF EXISTS cf_etl_table_wsl;
CREATE TABLE cf_etl_table_wsl
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

INSERT INTO cf_etl_table_wsl(schema_name, table_name, hdfs_upload_location, hdfs_file_name, start_date_time,
                         end_date_time, is_incremental, inc_field)
VALUES ('transaction_db', 'transaction', 'hdfs://172.24.240.1:19000//mydir/', 'transaction_wsl', '2023-06-23 00:00:00',
        '2023-06-23 23:59:59', 1, 'tnx_date');

SELECT *
FROM cf_etl_table_wsl;


