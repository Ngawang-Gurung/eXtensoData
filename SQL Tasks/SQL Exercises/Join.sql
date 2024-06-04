SHOW DATABASES;
USE exercise;

SHOW TABLES;

# 1
SELECT s.name AS Salesman, c.cust_name, s.city
FROM salesman s
         JOIN customer c ON s.city = c.city;

SELECT salesman.name AS Salesman, customer.cust_name, customer.city
FROM salesman,
     customer
WHERE salesman.city = customer.city;

# 2
SELECT o.ord_no, o.purch_amt, c.cust_name, c.city
FROM customer c
         JOIN orders o ON c.customer_id = o.customer_id
WHERE o.purch_amt BETWEEN 500 AND 20000;

# 3
SELECT customer.cust_name AS `Customer Name`, customer.city, salesman.name AS Salesman, salesman.commission
FROM customer
         JOIN salesman ON customer.salesman_id = salesman.salesman_id;

# 4
SELECT customer.cust_name AS `Customer Name`, customer.city, salesman.name AS Salesman, salesman.commission
FROM customer
         JOIN salesman ON customer.salesman_id = salesman.salesman_id
WHERE commission > 0.12;

# 5
SELECT customer.cust_name AS `Customer Name`,
       customer.city,
       salesman.name      AS Salesman,
       salesman.city,
       salesman.commission
FROM customer
         JOIN salesman ON customer.salesman_id = salesman.salesman_id
WHERE salesman.city != customer.city
  AND commission > 0.12;

# 6
SELECT orders.ord_no,
       orders.ord_date,
       orders.purch_amt,
       customer.cust_name,
       customer.grade,
       salesman.name AS Salesman,
       salesman.commission
FROM orders
         JOIN customer ON orders.customer_id = customer.customer_id
         JOIN salesman ON orders.salesman_id = salesman.salesman_id;

# 7
SELECT *
FROM salesman
         NATURAL JOIN customer
         NATURAL JOIN orders;

# 8
SELECT customer.cust_name, customer.city, customer.grade, salesman.name AS salesman, salesman.city
FROM customer
         JOIN salesman ON salesman.salesman_id = customer.salesman_id
ORDER BY customer.customer_id;

# 9
SELECT customer.cust_name, customer.city, customer.grade, salesman.name AS salesman, salesman.city
FROM customer
         LEFT JOIN salesman ON salesman.salesman_id = customer.salesman_id
WHERE customer.grade < 300
ORDER BY customer.customer_id;

# 10
SELECT customer.cust_name, customer.city, orders.ord_no, orders.ord_date, orders.purch_amt
FROM customer
         LEFT JOIN orders ON customer.customer_id = orders.customer_id
ORDER BY orders.ord_date;

# 11
SELECT customer.cust_name, customer.city, orders.ord_no, orders.ord_date, orders.purch_amt
FROM customer
         LEFT JOIN orders ON customer.customer_id = orders.customer_id
         LEFT JOIN salesman ON customer.salesman_id = salesman.salesman_id;

#12
SELECT salesman.salesman_id, salesman.name
FROM customer
         RIGHT JOIN salesman ON salesman.salesman_id = customer.salesman_id
ORDER BY salesman.salesman_id;

#13
SELECT salesman.name AS salesperson,
       customer.cust_name,
       customer.city,
       customer.grade,
       orders.ord_no,
       orders.ord_date,
       orders.purch_amt
FROM customer
         RIGHT JOIN salesman ON salesman.salesman_id = customer.salesman_id
         RIGHT JOIN orders ON customer.customer_id = orders.customer_id;

#14*
SELECT customer.cust_name, customer.grade, salesman.name AS Salesman, orders.purch_amt
FROM customer
         RIGHT JOIN salesman ON customer.salesman_id = salesman.salesman_id
         LEFT JOIN orders ON customer.customer_id = orders.customer_id
WHERE orders.purch_amt > 2000
  AND customer.grade IS NOT NULL;

#15
SELECT customer.cust_name, orders.ord_no, orders.ord_date, orders.purch_amt
FROM customer
         LEFT JOIN orders ON customer.customer_id = orders.customer_id;

#16
SELECT customer.cust_name, orders.ord_no, orders.ord_date, orders.purch_amt
FROM customer
          FULL JOIN orders ON customer.customer_id = orders.customer_id
WHERE customer.grade IS NOT NULL;

#17
SELECT *
FROM salesman
         CROSS JOIN customer;

#18
SELECT *
FROM salesman
         CROSS JOIN customer
WHERE salesman.city IS NOT NULL;

#19
SELECT *
FROM salesman
         CROSS JOIN customer
WHERE salesman.city IS NOT NULL
  AND customer.grade IS NOT NULL;

#20
SELECT *
FROM salesman
         CROSS JOIN customer
WHERE salesman.city IS NOT NULL
  AND customer.grade IS NOT NULL
  AND salesman.city <> customer.city;

#21
SELECT *
FROM company_mast
         JOIN item_mast ON company_mast.com_id = item_mast.pro_com;

#22
SELECT item_mast.pro_name, item_mast.pro_price, company_mast.com_name
FROM company_mast
         RIGHT JOIN item_mast ON company_mast.com_id = item_mast.pro_com;

#23
SELECT AVG(item_mast.pro_price), company_mast.com_name
FROM company_mast
         JOIN item_mast ON company_mast.com_id = item_mast.pro_com
GROUP BY company_mast.com_name;

#24
SELECT AVG(item_mast.pro_price) AS average_value, company_mast.com_name
FROM company_mast
         JOIN item_mast ON company_mast.com_id = item_mast.pro_com
GROUP BY company_mast.com_name
HAVING average_value > 350;

#25*
SELECT item_mast.pro_name, item_mast.pro_price, company_mast.com_name
FROM item_mast
         JOIN company_mast ON item_mast.pro_com = company_mast.com_id
WHERE item_mast.pro_price = (SELECT MAX(item_mast.pro_price)
                             FROM item_mast
                             WHERE item_mast.pro_com = company_mast.com_id);

SELECT item_mast.pro_name, item_mast.pro_price, company_mast.com_name
FROM item_mast
         JOIN company_mast ON item_mast.pro_com = company_mast.com_id
    AND item_mast.pro_price = (SELECT MAX(item_mast.pro_price)
                               FROM item_mast
                               WHERE item_mast.pro_com = company_mast.com_id);

#26
SELECT *
FROM emp_department
         JOIN emp_details ON emp_department.dpt_code = emp_details.emp_dept;

#27
SELECT emp_details.emp_fname, emp_details.emp_lname, emp_department.dpt_name, emp_department.dpt_allotment
FROM emp_details
         JOIN emp_department ON emp_dept = dpt_code;

#28
SELECT emp_department.dpt_name, emp_details.emp_fname, emp_details.emp_lname, emp_department.dpt_allotment
FROM emp_details
         JOIN emp_department ON emp_details.emp_dept = emp_department.dpt_code
WHERE dpt_allotment > 50000;

#29*
SELECT emp_department.dpt_name
FROM emp_department
         JOIN emp_details ON emp_department.dpt_code = emp_details.emp_dept
GROUP BY emp_department.dpt_name
HAVING COUNT(*) > 2;

SELECT emp_department.dpt_name, GROUP_CONCAT(emp_details.emp_fname) AS employee_names
FROM emp_department
         JOIN emp_details ON emp_department.dpt_code = emp_details.emp_dept
GROUP BY emp_department.dpt_name
HAVING COUNT(*) > 2;