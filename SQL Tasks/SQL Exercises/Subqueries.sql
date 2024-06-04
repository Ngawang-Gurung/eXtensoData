USE exercise;

SHOW TABLES;

#1
SELECT *
FROM orders
WHERE salesman_id IN (SELECT salesman_id
                     FROM salesman
                     WHERE salesman.name = "Paul Adam");
#2
SELECT *
FROM orders
WHERE salesman_id IN (SELECT salesman_id
                     FROM salesman
                     WHERE city = "London");

#3
SELECT *
FROM orders
WHERE salesman_id IN (
    SELECT salesman_id FROM orders WHERE customer_id = 3007
    );

#4
SELECT *
FROM
    orders WHERE purch_amt >
                 (SELECT avg(purch_amt) FROM orders WHERE ord_date = '2012-10-10');

#5
SELECT *
FROM orders
WHERE salesman_id IN (
    SELECT salesman_id FROM salesman WHERE city = "New York"
    );

#6







