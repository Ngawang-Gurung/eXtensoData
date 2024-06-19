USE client_rw;
SHOW tables;

-- Simple Stored Procedure --

DELIMITER //
DROP PROCEDURE IF EXISTS p //
CREATE PROCEDURE p(
    IN branch_ INT,
    IN lcy_amount_ DOUBLE
)
BEGIN
    SELECT *
    FROM fc_transaction_base
    WHERE branch = branch_
    AND lcy_amount = lcy_amount_;
END //
DELIMITER ;

CALL p(15, 20000);


-- Fill Date using Loop --

USE mydb;

DROP TABLE IF EXISTS calendars;
CREATE TABLE calendars (
    date DATE PRIMARY KEY,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

DELIMITER //
DROP PROCEDURE IF EXISTS fillDate //
CREATE PROCEDURE fillDate(IN startDate DATE, IN endDate DATE)
BEGIN
    DECLARE currentDate DATE DEFAULT startDate;

    insert_data:LOOP
        SET currentDate = DATE_ADD(currentDate, INTERVAL 1 DAY );

    IF currentDate > endDate THEN
        LEAVE insert_data;
    END IF;

    INSERT INTO calendars(date, month, quarter, year)
        VALUES (currentDate, month(currentDate), quarter(currentDate), year(currentDate));
    END LOOP insert_data;
END //

DELIMITER ;

CALL fillDate('2025-01-01', '2025-12-31');

SELECT * FROM calendars LIMIT 5;

-- Multiplication Chart from 1 to 5 using Loop --

DROP TABLE IF EXISTS multiple;
CREATE TABLE multiple(
    one int,
    two int,
    three int,
    four int,
    five int
);

DELIMITER //
DROP PROCEDURE IF EXISTS multiplication //
CREATE PROCEDURE multiplication(in lower int, in upper int)
BEGIN
    DECLARE val int default lower;
    insert_val: LOOP
        set val = val+1;
    if val > upper then
        leave insert_val;
    END IF;

        INSERT INTO multiple(one, two, three, four, five) VALUES (val, val*2, val*3, val*4, val*5);

    END LOOP;
END //
DELIMITER ;

call multiplication(0, 10);

SELECT * FROM multiple;

-- While Loop --

DELIMITER //
CREATE PROCEDURE insertCalender(IN currentDate DATE)
BEGIN
    INSERT INTO calendars VALUES (currentDate, month(currentDate), quarter(currentDate), year(currentDate));
END //
DELIMITER ;

DELIMITER //
DROP PROCEDURE IF EXISTS loadDates //
CREATE PROCEDURE loadDates(IN startDate DATE, IN day INT)
BEGIN
    DECLARE counter INT DEFAULT 0;
    DECLARE currentDate DATE DEFAULT startDate;

    my_while: WHILE counter < day DO
            CALL insertCalender(currentDate);
            SET counter = counter + 1;
            SET currentDate = DATE_ADD(currentDate, INTERVAL 1 day);
        END WHILE my_while;
END //
DELIMITER ;

CALL loadDates('2023-01-01', 365);

SELECT * FROM calendars;

-- Repeat Loop --

DELIMITER $$

CREATE PROCEDURE RepeatDemo()
BEGIN
    DECLARE counter INT DEFAULT 1;
    DECLARE result VARCHAR(100) DEFAULT '';

    REPEAT
        SET result = CONCAT(result,counter,',');
        SET counter = counter + 1;
    UNTIL counter >= 10
    END REPEAT;

    -- display result
    SELECT result;
END$$

DELIMITER ;

CALL RepeatDemo();

-- Cursor --

DELIMITER //
DROP PROCEDURE IF EXISTS create_five_list;
CREATE PROCEDURE create_five_list (INOUT five_list TEXT)
BEGIN
    DECLARE done BOOL DEFAULT FALSE;
    DECLARE number varchar(100) DEFAULT "";

    DECLARE cur CURSOR FOR SELECT five FROM multiple;
    DECLARE CONTINUE HANDLER FOR NOT FOUND set done = TRUE;

    OPEN cur;

    SET five_list = '';

    process_five: LOOP
        FETCH cur INTO number;
        IF done THEN
            LEAVE process_five;
        END IF ;
        SET five_list = concat(five_list, number, ";");
    END LOOP process_five;
    CLOSE cur;
END //
DELIMITER ;

CALL create_five_list(@five_list);

SELECT @five_list;

SELECT multiple.five FROM multiple;