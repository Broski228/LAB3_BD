
CREATE DATABASE advanced_lab;

CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50) DEFAULT 'General',
    salary INTEGER DEFAULT 40000,
    hire_date DATE,
    status VARCHAR(20) DEFAULT 'Active'
);

DROP TABLE IF EXISTS departments CASCADE ;

CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(50),
    budget INTEGER,
    manager_id INTEGER
);

CREATE TABLE projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(100),
    dept_id INTEGER,
    start_date DATE,
    end_date DATE,
    budget INTEGER
);


INSERT INTO employees (first_name, last_name, department)
VALUES ('John', 'Doe','IT');

INSERT INTO employees  (first_name, last_name, department)
VALUES ('Anna','Smith',DEFAULT);

INSERT INTO departments (dept_name, budget, manager_id)
VALUES ('IT',150000,1), ('HR', 80000, 2), ('Salrs',120000,3);

INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES ('Mike', 'Brown', 'Finance', 50000 * 1.1, CURRENT_DATE);

CREATE TEMP TABLE temp_employees AS
SELECT * FROM employees WHERE department = 'IT';



UPDATE employees
SET salary = salary * 1.1
WHERE emp_id IS NOT NULL;

UPDATE employees
SET status = 'Senior'
WHERE salary > 60000 AND hire_date < '2020-01-01';

UPDATE employees
SET department = CASE
    WHEN salary > 80000 THEN 'Management'
    WHEN salary BETWEEN 50000 AND 80000 THEN 'Senior'
    ELSE 'Junior'
END;

UPDATE employees SET department = DEFAULT WHERE status = 'Inactive';

UPDATE departments d
SET budget = (SELECT AVG(salary) * 1.2
              FROM employees e
              WHERE e.department = d.dept_name);

UPDATE employees
SET salary = salary * 1.15, status = 'Promoted'
WHERE department = 'Sales';




DELETE FROM employees WHERE status = 'Terminated';

DELETE FROM employees
WHERE salary < 40000 AND hire_date > '2023-01-01' AND department IS NULL;


DELETE FROM departments
WHERE dept_name NOT IN (
    SELECT DISTINCT department FROM employees WHERE department IS NOT NULL
);

DELETE FROM projects
WHERE end_date < '2023-01-01'
RETURNING * ;


INSERT INTO employees (first_name, last_name, salary, department)
VALUES ('Null', 'Case', NULL, NULL);

UPDATE employees
SET department = 'Unassigned'
WHERE department IS NULL;

DELETE FROM employees
WHERE salary IS NULL OR department IS NULL;

INSERT INTO employees (first_name, last_name,department)
VALUES ('Sarah', 'Connor','HR')
RETURNING emp_id,first_name || ' ' || last_name AS full_name;

UPDATE employees
SET salary =salary + 5000
WHERE department = 'IT'
RETURNING emp_id, salary - 5000 AS old_salary, salary AS new_salary;

DELETE FROM employees
WHERE hire_date<'2020-01-01'
RETURNING *;





INSERT INTO employees (first_name, last_name, department)
SELECT 'Alex', 'White', 'IT'
WHERE NOT EXISTS (
    SELECT 1 FROM employees WHERE first_name = 'Alex' AND last_name = 'White'
);

UPDATE employees e
SET salary = salary * (CASE WHEN d.budget > 100000 THEN 1.1 ELSE 1.05 END)
FROM departments d
WHERE e.department = d.dept_name;

INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES
('Emp1','Last1', 'IT', 50000,CURRENT_DATE),
('Emp2','Last2','IT', 50000, CURRENT_DATE),
('Emp3', 'Last3', 'IT', 50000,CURRENT_DATE),
('Emp4','Last4','IT', 50000, CURRENT_DATE),
('Emp5','Last5', 'IT', 50000, CURRENT_DATE);

UPDATE employees
SET salary = salary * 1.1
WHERE first_name LIKE 'Emp%';

CREATE TABLE employee_archive AS TABLE employees WITH NO DATA;

INSERT INTO employee_archive SELECT * FROM employees WHERE status = 'Inactive';
DELETE FROM employees WHERE status ='Inactive';

UPDATE projects p
SET end_date = end_date + INTERVAL '30 days'
WHERE budget > 50000
AND (
    SELECT COUNT(*)
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    WHERE d.dept_id = p.dept_id
) > 3;
