SHOW DATABASES ;
USE config_db;

DROP TABLE IF EXISTS cf_field_mapping;
CREATE TABLE cf_field_mapping
(
    id          int,
    source_name varchar(200),
    dest_name   varchar(200)
);

INSERT INTO cf_field_mapping (id, source_name, dest_name)
VALUES (1, 'txn_id', 'transaction_id'),
       (1, 'txn_date', 'transaction_date'),
       (1, 'acc_id', 'account_id'),
       (1, 'product', 'product_name'),
       (1, 'DATE(txn_date)', 'trans_date_only');

SELECT *
FROM cf_field_mapping;

DELIMITER //
DROP PROCEDURE IF EXISTS sp_field_mapping //
CREATE PROCEDURE sp_field_mapping()
BEGIN
    DECLARE done bool DEFAULT FALSE;
    DECLARE source_col varchar(200);
    DECLARE dest_col varchar(200);

    DECLARE cur CURSOR FOR SELECT source_name, dest_name FROM cf_field_mapping;
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
    SET @sql_query = CONCAT('SELECT ', @query, ' FROM transaction_db.transaction');
    PREPARE statement FROM @sql_query;
    EXECUTE statement;
    DEALLOCATE PREPARE statement;
END //
DELIMITER ;

CALL sp_field_mapping();



