USE customer;

SHOW TABLES;

-- Using stored procedure to describe a column --

DELIMITER //
DROP PROCEDURE IF EXISTS describe_column //
CREATE PROCEDURE describe_column(IN tableName varchar(255), IN columnName varchar(255))
BEGIN
    SET @sql_query = CONCAT('SELECT ''', columnName, ''' AS column_name,
                        AVG(', columnName, ') AS avg,
                        STD(', columnName, ') AS std,
                        MIN(', columnName, ') AS min,
                        MAX(', columnName, ') AS max,
                        SUM(', columnName, ') AS sum,
                        COUNT(DISTINCT ', columnName, ') AS distinct_count,
                        (SELECT COUNT(*) FROM ', tableName, ' WHERE ', columnName, ' IS NULL) AS null_count
                        FROM ', tableName);
    PREPARE statement FROM @sql_query;
    EXECUTE statement;
    DEALLOCATE PREPARE statement;
END //

DELIMITER ;

CALL describe_column('rw_transaction_data', 'amount');

-- Find all column names of a table in database

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'new_customer_profile'
  AND table_schema = 'customer';

-- Describe Table --

DELIMITER //
DROP PROCEDURE IF EXISTS describe_table //
CREATE PROCEDURE describe_table(IN tableName VARCHAR(255))
BEGIN
    DECLARE done BOOL DEFAULT FALSE;
    DECLARE columnName VARCHAR(255);

    DECLARE cur CURSOR FOR
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = tableName;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET @sql_query = '';
    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO columnName;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SET @sql_query_part = CONCAT(
            'SELECT ''', columnName, ''' AS column_name, ',
            'AVG(`', columnName, '`) AS avg_value, ',
            'STD(`', columnName, '`) AS std_value, ',
            'MIN(`', columnName, '`) AS min_value, ',
            'MAX(`', columnName, '`) AS max_value, ',
            'SUM(`', columnName, '`) AS sum_value, ',
            'COUNT(DISTINCT `', columnName, '`) AS distinct_count, ',
            '(SELECT COUNT(*) FROM `', tableName, '` WHERE `', columnName, '` IS NULL) AS null_count ',
            'FROM `', tableName, '`'
        );

        -- Add UNION ALL only if the query is not empty
        IF @sql_query != '' THEN
            SET @sql_query = CONCAT(@sql_query, ' UNION ALL ');
        END IF;

        -- Append the current query part to the main query
        SET @sql_query = CONCAT(@sql_query, @sql_query_part);

    END LOOP;

    CLOSE cur;

    PREPARE statement FROM @sql_query;
    EXECUTE statement;
    DEALLOCATE PREPARE statement;
END //
DELIMITER ;

SHOW TABLES ;

CALL describe_table('rw_transaction_data');

