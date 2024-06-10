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
WHERE salesman_id IN (SELECT salesman_id
                      FROM orders
                      WHERE customer_id = 3007);

#4*
SELECT *
FROM orders
WHERE purch_amt >
      (SELECT AVG(purch_amt) FROM orders WHERE ord_date = '2012-10-10');

#5
SELECT *
FROM orders
WHERE salesman_id IN (SELECT salesman_id
                      FROM salesman
                      WHERE city = "New York");

#6
SELECT salesman.commission
FROM salesman
WHERE salesman_id IN (SELECT salesman_id
                      FROM customer
                      WHERE city = 'Paris');

#7*
SELECT customer.customer_id, customer.cust_name
FROM customer
WHERE customer.customer_id =
      (SELECT salesman_id - 2001 FROM salesman WHERE name = "Mc Lyon");

#8*
SELECT grade, COUNT(*)
FROM customer
WHERE grade > (SELECT AVG(grade) FROM customer WHERE city = 'New York')
GROUP BY grade;

#9*
SELECT orders.ord_no, orders.ord_date, orders.salesman_id
FROM orders
         JOIN salesman ON orders.salesman_id = salesman.salesman_id
WHERE commission = (SELECT MAX(commission)
                    FROM salesman);

-- Which is better?--
SELECT orders.ord_no, orders.ord_date, orders.salesman_id
FROM orders
WHERE salesman_id IN (SELECT salesman_id
                      FROM salesman
                      WHERE commission = (SELECT MAX(commission)
                                          FROM salesman));

#10
SELECT o.*, c.cust_name
FROM orders o
         JOIN customer c
              ON o.customer_id = c.customer_id
WHERE o.ord_date = '2012-08-17';

#11*
SELECT salesman.salesman_id, salesman.name
FROM salesman
WHERE 1 < (SELECT COUNT(*) FROM customer WHERE customer.salesman_id = salesman.salesman_id);

#12*

SELECT *
FROM orders a
WHERE purch_amt >
      (SELECT AVG(purch_amt)
       FROM orders b
       WHERE b.customer_id = a.customer_id);

#13 - Similar to 12

#14
SELECT SUM(a.purch_amt) sums, a.ord_date
FROM orders a
GROUP BY a.ord_date
HAVING sums > (SELECT 1000 + MAX(purch_amt) FROM orders b WHERE a.ord_date = b.ord_date);

#15
SELECT *
FROM customer
WHERE 0 < (SELECT COUNT(*)
           FROM customer
           WHERE city = "London");

SELECT *
FROM customer
WHERE EXISTS(SELECT *
             FROM customer
             WHERE city = "London");

#16*
#17*





