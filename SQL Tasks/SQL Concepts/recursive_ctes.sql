USE mydb;

SHOW TABLES;

# Series of first 5 odd numbers

WITH RECURSIVE odd_no (sr_no, n) AS
                   (SELECT 1, 1
                    UNION ALL
                    SELECT sr_no + 1, n + 2
                    FROM odd_no
                    WHERE sr_no < 5)
SELECT *
FROM odd_no;

# Multiplication up to 5

WITH RECURSIVE multiplication ( n1, n2, n3, n4, n5) AS
                   (SELECT 1, 2, 3, 4, 5
                    UNION ALL
                    SELECT n1 + 1, n1 + 2, n3 + 3, n4 + 4, n5 + 5
                    FROM multiplication
                    WHERE n1 < 10)
SELECT *
FROM multiplication;

