--Part 1
CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    dept_id INT ,
    salary DECIMAL (10,2)
);

CREATE TABLE departments (
dept_id INT PRIMARY KEY,
dept_name VARCHAR(50),
location VARCHAR(50)
);
DROP TABLE projects CASCADE
CREATE TABLE projects (
project_id INT PRIMARY KEY,
project_name VARCHAR(50),
dept_id INT,
budget DECIMAL(10, 2)
);

INSERT INTO employees (emp_id, emp_name, dept_id, salary)
VALUES
(1, 'John Smith', 101, 50000),
(2, 'Jane Doe', 102, 60000),
(3, 'Mike Johnson', 101, 55000),
(4, 'Sarah Williams', 103, 65000),
(5, 'Tom Brown', NULL, 45000);

INSERT INTO departments (dept_id, dept_name, location) VALUES
(101, 'IT', 'Building A'),
(102, 'HR', 'Building B'),
(103, 'Finance', 'Building C'),
(104, 'Marketing', 'Building D');

INSERT INTO projects (project_id, project_name, dept_id,
budget) VALUES
(1, 'Website Redesign', 101, 100000),
(2, 'Employee Training', 102, 50000),
(3, 'Budget Analysis', 103, 75000),
(4, 'Cloud Migration', 101, 150000),
(5, 'AI Research', NULL, 200000);

--part 2

-- ex 2.1

SELECT e.emp_name, d.dept_name
FROM employees e
         CROSS JOIN departments d;

-- 5 * 4 = 20 rows

-- ex 2.2

SELECT e.emp_name , d.dept_name
FROM employees e , departments d;

SELECT e.emp_name , d.dept_name
FROM employees e
INNER JOIN departments d
ON TRUE;

--ex 2.3

SELECT
    e.emp_name AS employee,
    p.project_name AS project
FROM
    employees e
CROSS JOIN projects p
ORDER BY
    e.emp_name , p.project_name;

-- part 3

-- ex 3.1

SELECT e.emp_name , d.dept_name , d.location
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id;

-- 4 rows are returned , Tom Browns is not included because he has not any dept_id !

-- ex 3.2

SELECT emp_name , dept_name, location
FROM employees
INNER JOIN departments USING (dept_Id) ;
-- In the first case u will see two dept_id columns , while the second case will show you only one merged column

--ex 3.3
SELECT emp_name, dept_name, location
FROM employees
NATURAL INNER JOIN departments;

--ex 3.4

SELECT e.emp_name, d.dept_name , p.project_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id
INNER JOIN projects p ON  d.dept_id = p.dept_id;

--PART 4 LEFT JOIN

--ex 4.1

SELECT e.emp_name, e.dept_id AS emp_dept , d.dept_id AS
dept_dept , d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;

-- He has null results

--ex 4.2

SELECT e.emp_name , e.dept_id AS emp_dept, d.dept_id as
dept_dept, d.dept_name
FROM employees e
LEFT JOIN departments d
USING (dept_id) ;

--ex 4.3

SELECT e.emp_name, e.dept_id
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
WHERE d.dept_id IS NULL;

--ex 4.4

SELECT d.dept_name, COUNT (e.emp_id) AS employee_count
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name
ORDER BY employee_count DESC;

--part 5 right join
SELECT e.emp_name , d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id;

-- ex 5.2

SELECT e.emp_name , d.dept_name
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
ORDER BY d.dept_id, d.dept_name DESC;

-- ex 5.3

SELECT d.dept_name , d.location
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id
WHERE e.emp_id is NULL;

--part 6 full join

SELECT e.emp_name , e.dept_id AS emp_dept , d.dept_id AS dept_dept, d.dept_name
FROM employees e
FULL JOIN departments d ON e.dept_id = d.dept_id;

--Tom Brown on the left and Marketing on the right

--ex 6.2
SELECT d.dept_name , p.project_name , p.budget
FROM departments d
FULL JOIN projects p ON d.dept_id = p.dept_id;

--ex 6.3
SELECT
    CASE
        WHEN e.emp_id is NULL THEN 'Department without employees'
        WHEN d.dept_id is NULL THEN 'Employee without department'
        ELSE 'Matched'
    END AS record_status,
    e.emp_name,
    d.dept_name
FROM employees e
FULL JOIN departments d ON e.dept_id = d.dept_id
WHERE e.emp_id IS NULL OR d.dept_id is NULL;

--part 7

SELECT e.emp_name , d.dept_name, e.salary
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id AND d.location = 'Building A';

-- ex 7.2

SELECT e.emp_name , d.dept_name , e.salary
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
WHERE d.location = 'Building A';

--In Query 1 we can see all of employees , but only Building A employees are matched, while in Query2 we see only matched employees, no null values!

--ex 7.3

SELECT e.emp_name , d.dept_name, e.salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id and d.location = 'Building A';


SELECT e.emp_name , d.dept_name , e.salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE d.location = 'Building A';

-- So INNER JOIN shows only matched values as well as WHERE clause, thats why they are no differences

--part 8
SELECT
    d.dept_name,
    e.emp_name,
    e.salary,
    p.project_name,
    p.budget
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
LEFT JOIN projects p ON d.dept_id = p.dept_id
ORDER BY d.dept_name , e.emp_name ;

--ex 8.2
ALTER TABLE employees ADD COLUMN manager_id INT;

UPDATE employees SET manager_id = 3 WHERE emp_id = 1;
UPDATE employees SET manager_id = 3 WHERE emp_id = 2;
UPDATE employees SET manager_id = NULL WHERE emp_id = 3;
UPDATE employees SET manager_id = 3 where emp_id = 4;
update employees set manager_id = 3 where emp_id = 5;

SELECT
    e.emp_name AS employee,
    m.emp_name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id;

SELECT d.dept_name , AVG(e.salary) AS avg_salary
FROM departments d
INNER JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id , d.dept_name
HAVING AVG(e.salary) > 50000;


-- QUESTIONS

/*
 1. Inner Join returns only rows that match in both tables, Left join returns all rows from the left table , and matching rows from the right one
 2. For example creating a schedule , testing all possible pairs
 3. For Inner join , the filter gives the same result because it will show only matching rows anyway
    For outer join , ON filter is applied during the join , unmatched rows can still appear as null , WHERE filter applied after the join , null rows can be removed
 4. 5 * 10 = 50 rows
 5. It automatically joins on all columns that have the same name in both tables
 6. It may join on unexpected columns with the same name , if the table structure changes , the join can produce wrong results
 7. SELECT * FROM B RIGHT JOIN A ON A.id = B.id;
 8. When you need all rows from both tables
 */

