-- Creating Tables --

SHOW DATABASES ;
CREATE DATABASE exercise;
USE exercise;

-- Creating the salesman table
CREATE TABLE salesman (
    salesman_id INT PRIMARY KEY,
    name VARCHAR(50),
    city VARCHAR(50),
    commission DECIMAL(5,2)
);

-- Inserting data into the salesman table
INSERT INTO salesman (salesman_id, name, city, commission) VALUES
(5001, 'James Hoog', 'New York', 0.15),
(5002, 'Nail Knite', 'Paris', 0.13),
(5005, 'Pit Alex', 'London', 0.11),
(5006, 'Mc Lyon', 'Paris', 0.14),
(5007, 'Paul Adam', 'Rome', 0.13),
(5003, 'Lauson Hen', 'San Jose', 0.12);

-- Creating the customer table
CREATE TABLE customer (
    customer_id INT PRIMARY KEY,
    cust_name VARCHAR(50),
    city VARCHAR(50),
    grade INT,
    salesman_id INT,
    FOREIGN KEY (salesman_id) REFERENCES salesman(salesman_id)
);

-- Inserting data into the customer table
INSERT INTO customer (customer_id, cust_name, city, grade, salesman_id) VALUES
(3002, 'Nick Rimando', 'New York', 100, 5001),
(3007, 'Brad Davis', 'New York', 200, 5001),
(3005, 'Graham Zusi', 'California', 200, 5002),
(3008, 'Julian Green', 'London', 300, 5002),
(3004, 'Fabian Johnson', 'Paris', 300, 5006),
(3009, 'Geoff Cameron', 'Berlin', 100, 5003),
(3003, 'Jozy Altidor', 'Moscow', 200, 5007),
(3001, 'Brad Guzan', 'London', NULL, 5005);

-- Creating the orders table
CREATE TABLE orders (
    ord_no INT PRIMARY KEY,
    purch_amt DECIMAL(10,2),
    ord_date DATE,
    customer_id INT,
    salesman_id INT,
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (salesman_id) REFERENCES salesman(salesman_id)
);

-- Inserting data into the orders table
INSERT INTO orders (ord_no, purch_amt, ord_date, customer_id, salesman_id) VALUES
(70001, 150.5, '2012-10-05', 3005, 5002),
(70009, 270.65, '2012-09-10', 3001, 5005),
(70002, 65.26, '2012-10-05', 3002, 5001),
(70004, 110.5, '2012-08-17', 3009, 5003),
(70007, 948.5, '2012-09-10', 3005, 5002),
(70005, 2400.6, '2012-07-27', 3007, 5001),
(70008, 5760, '2012-09-10', 3002, 5001),
(70010, 1983.43, '2012-10-10', 3004, 5006),
(70003, 2480.4, '2012-10-10', 3009, 5003),
(70012, 250.45, '2012-06-27', 3008, 5002),
(70011, 75.29, '2012-08-17', 3003, 5007),
(70013, 3045.6, '2012-04-25', 3002, 5001);

-- Creating the company_mast table
CREATE TABLE company_mast (
    com_id INT PRIMARY KEY,
    com_name VARCHAR(50)
);

-- Inserting data into the company_mast table
INSERT INTO company_mast (com_id, com_name) VALUES
(11, 'Samsung'),
(12, 'iBall'),
(13, 'Epsion'),
(14, 'Zebronics'),
(15, 'Asus'),
(16, 'Frontech');

-- Creating the item_mast table
CREATE TABLE item_mast (
    pro_id INT PRIMARY KEY,
    pro_name VARCHAR(50),
    pro_price DECIMAL(10,2),
    pro_com INT,
    FOREIGN KEY (pro_com) REFERENCES company_mast(com_id)
);

-- Inserting data into the item_mast table
INSERT INTO item_mast (pro_id, pro_name, pro_price, pro_com) VALUES
(101, 'Mother Board', 3200.00, 15),
(102, 'Key Board', 450.00, 16),
(103, 'ZIP drive', 250.00, 14),
(104, 'Speaker', 550.00, 16),
(105, 'Monitor', 5000.00, 11),
(106, 'DVD drive', 900.00, 12),
(107, 'CD drive', 800.00, 12),
(108, 'Printer', 2600.00, 13),
(109, 'Refill cartridge', 350.00, 13),
(110, 'Mouse', 250.00, 12);

-- Creating the emp_department table
CREATE TABLE emp_department (
    dpt_code INT PRIMARY KEY,
    dpt_name VARCHAR(50),
    dpt_allotment DECIMAL(10,2)
);

-- Inserting data into the emp_department table
INSERT INTO emp_department (dpt_code, dpt_name, dpt_allotment) VALUES
(57, 'IT', 65000),
(63, 'Finance', 15000),
(47, 'HR', 240000),
(27, 'RD', 55000),
(89, 'QC', 75000);

-- Creating the emp_details table
CREATE TABLE emp_details (
    emp_idno INT PRIMARY KEY,
    emp_fname VARCHAR(50),
    emp_lname VARCHAR(50),
    emp_dept INT,
    FOREIGN KEY (emp_dept) REFERENCES emp_department(dpt_code)
);

-- Inserting data into the emp_details table
INSERT INTO emp_details (emp_idno, emp_fname, emp_lname, emp_dept) VALUES
(127323, 'Michale', 'Robbin', 57),
(526689, 'Carlos', 'Snares', 63),
(843795, 'Enric', 'Dosio', 57),
(328717, 'Jhon', 'Snares', 63),
(444527, 'Joseph', 'Dosni', 47),
(659831, 'Zanifer', 'Emily', 47),
(847674, 'Kuleswar', 'Sitaraman', 57),
(748681, 'Henrey', 'Gabriel', 47),
(555935, 'Alex', 'Manuel', 57),
(539569, 'George', 'Mardy', 27),
(733843, 'Mario', 'Saule', 63),
(631548, 'Alan', 'Snappy', 27),
(839139, 'Maria', 'Foster', 57);

-- Joins--

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

-- Subqueries --

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

