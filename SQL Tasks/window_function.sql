-- Learning Window Functions

SHOW DATABASES;
CREATE DATABASE industry;
USE industry;

CREATE TABLE employee (
    name varchar(255),
    age int,
    department varchar(255),
    salary int
);

DROP TABLE employee;

SHOW TABLES ;

DESCRIBE employee;

INSERT INTO employee VALUES ('Ramesh', 20, 'Finance', 50000);
INSERT INTO employee VALUES ('Deep', 25, 'Sales', 30000);
INSERT INTO employee VALUES ('Suresh', 22, 'Finance', 50000);
INSERT INTO employee VALUES ('Ram', 28, 'Finance', 20000);
INSERT INTO employee VALUES ('Pradeep', 22, 'Sales', 20000);

SELECT * FROM employee;

-- Find avg salary for each department and order employees within a department by age

SELECT department, avg(salary) FROM employee GROUP BY department;

SELECT
    employee.*, avg(employee.salary)
                    OVER (PARTITION BY employee.department ORDER BY age)
        AS avg_salary
FROM employee;

SELECT
    ROW_NUMBER() OVER (PARTITION BY Department ORDER BY Salary DESC) AS emp_row_no,
    Name,
    Department,
    Salary,
    RANK() OVER(PARTITION BY Department ORDER BY Salary DESC) AS emp_rank,
    DENSE_RANK() OVER(PARTITION BY Department ORDER BY Salary DESC) AS emp_dense_rank
FROM
    employee
