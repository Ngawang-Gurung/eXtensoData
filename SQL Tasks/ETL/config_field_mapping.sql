SHOW DATABASES;
USE config_db;

DROP TABLE IF EXISTS cf_field_mapping;
CREATE TABLE cf_field_mapping
(
    table_id    int,
    source_name varchar(200),
    dest_name   varchar(200)
);

INSERT INTO cf_field_mapping (table_id, source_name, dest_name)
VALUES (1, 'txn_id', 'transaction_id'),
       (1, 'txn_date', 'transaction_date'),
       (1, 'acc_id', 'account_id'),
       (1, 'product', 'product_name'),
       (1, 'DATE(txn_date)', 'trans_date_only'),
       (2, 'txn_id', 'transaction_id'),
       (2, 'acc_id', 'account_id'),
       (2, 'product', 'product_name');

SELECT *
FROM cf_field_mapping;

DELIMITER //
DROP PROCEDURE IF EXISTS config_db.sp_field_mapping //
CREATE PROCEDURE config_db.sp_field_mapping(IN schema_name varchar(50), IN table_name varchar(50), IN table_id int)
BEGIN
    DECLARE done bool DEFAULT FALSE;
    DECLARE source_col varchar(200);
    DECLARE dest_col varchar(200);

    DECLARE cur CURSOR FOR SELECT source_name, dest_name FROM config_db.cf_field_mapping WHERE config_db.cf_field_mapping.table_id = table_id ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;

    SET @query = '';
    my_loop:
    LOOP
        FETCH cur INTO source_col, dest_col;
        IF done THEN
            LEAVE my_loop;
        END IF;
        #         SET @query = CONCAT(@query,
#                             IF(LENGTH(@query) > 0, ', ', ''),
#                             IF(dest_col = 'transaction_date',
#                                CONCAT(source_col, ' AS ', dest_col, ', ', 'DATE(', source_col, ') AS dates'),
#                                CONCAT(source_col, ' AS ', dest_col)));
        SET @query = CONCAT(@query,
                            IF(LENGTH(@query) > 0, ', ', ''), source_col, ' AS ', dest_col);
    END LOOP;
    CLOSE cur;
    SET @sql_query = CONCAT('SELECT ', @query, ' FROM ', schema_name, '.', table_name);
    PREPARE statement FROM @sql_query;
    EXECUTE statement;
    DEALLOCATE PREPARE statement;
END //
DELIMITER ;

SELECT @sql_query;

CALL config_db.sp_field_mapping('transaction_db', 'transaction', 1);

SELECT group_concat(cf_field_mapping.source_name, ' AS ', cf_field_mapping.dest_name) FROM cf_field_mapping WHERE table_id = 1
INTO @as_query;

SELECT @as_query;
