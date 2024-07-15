SHOW DATABASES;
CREATE DATABASE mydb;

USE mydb;

SHOW TABLES;

CREATE TABLE dept
(
    deptname VARCHAR(255),
    manager  VARCHAR(255)
);

INSERT INTO dept
VALUES ('sales', 'bob'),
       ('sales', 'thomas'),
       ('production', 'katie'),
       ('production', 'mark');

CREATE TABLE employee
(
    name     VARCHAR(255),
    empid    INT,
    deptname VARCHAR(255)
);

INSERT INTO employee
VALUES ('Harry', 3415, 'Finance'),
       ('Sally', 2241, 'Sales'),
       ('George', 3401, 'Finance'),
       ('Harriet', 2202, 'Production');

SELECT *
FROM dept;
SELECT *
FROM employee;

-- Semi Join --
SELECT *
FROM employee
WHERE deptname IN (SELECT deptname
                   FROM dept);

SELECT *
FROM employee
WHERE exists(SELECT *
             FROM dept
             WHERE employee.deptname = dept.deptname);

-- Anti Join --
SELECT *
FROM employee
WHERE deptname NOT IN
      (SELECT deptname
       FROM dept);

SELECT *
FROM employee
WHERE NOT exists(SELECT *
                 FROM dept
                 WHERE employee.deptname = dept.deptname);

