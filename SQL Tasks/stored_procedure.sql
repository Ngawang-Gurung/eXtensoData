USE client_rw;
SHOW tables;

DELIMITER //

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


DROP PROCEDURE IF EXISTS p;



