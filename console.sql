--part 2
--ex 2.1
create index emp_salary_idx on employees(salary);

select indexname, indexdef
from pg_indexes
where tablename = 'employees';

--we have 2 indexes, the first one is a primary kry index , and the second one is the one we created.

--ex 2.2
create index emp_dept_idx on employees(dept_id);
SELECT * FROM employees WHERE dept_id = 101;

--indexing foreign keys improves JOIN performance and speeds up referential integrity checks

--ex 2.3
select
    tablename,
    indexname,
    indexdef
from pg_indexes
where schemaname = 'public'
order by tablename, indexname;

/*
 INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('assignments_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('book_catalog_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('bookings_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('categories_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('class_schedule_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('course_enrollments_student_id_course_code_semester_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('courses_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('customers2_email_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('customers2_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('departments_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('dept_summary_mv_idx');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('digital_downloads_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('emp_dept_idx');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('emp_salary_idx');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('employees_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('employees_dept_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('grade_scale_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('inventory_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('movies_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('movies_title_release_date_director_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('order_details_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('order_items_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('orders_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('orders2_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('products_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('products2_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('products_catalog_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('products_fk_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('professors_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('projects_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('reading_sessions_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('sales_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('screens_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('screens_theater_id_screen_number_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('semester_calendar_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('shariki_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('showtimes_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('showtimes_screen_id_show_date_start_time_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('student_courses_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('student_records_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('students_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('theaters_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('theaters_theater_name_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('unique_email');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('unique_username');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('users_email_key');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('users_pkey');
INSERT INTO pg_catalog.pg_indexes (indexname) VALUES ('users_username_key');
that ones which have pkey are created automatically
 */

 --part 3
CREATE INDEX emp_dept_salary_idx on employees(dept_id,salary);

select emp_name , salary
from employees
where dept_id = 101 and salary > 52000;

--no, because the index is sorted by dept_id first, so searching by salary alone requires a full scan

--ex 3.2
create index emp_salary_dept_idx on employees (salary,dept_id);

select * from employees where dept_id = 102 and salary > 50000;

select * from employees where salary > 50000 and dept_id = 102;

--yes, because the index can only be used effectively when the query includes the leading column

--part 4

ALTER TABLE employees ADD COLUMN email VARCHAR(100);
UPDATE employees SET email = 'john.smith@company.com' WHERE emp_id = 1;
UPDATE employees SET email = 'jane.doe@company.com' WHERE emp_id = 2;
UPDATE employees SET email = 'mike.johnson@company.com' WHERE emp_id = 3;
UPDATE employees SET email = 'sarah.williams@company.com' WHERE emp_id = 4;
UPDATE employees SET email = 'tom.brown@company.com' WHERE emp_id = 5;

create unique index emp_mail_unique_idx on employees(email);

insert into employees (emp_id, emp_name, dept_id, salary,email)
values (6,'New Employee', 101, 55000,'john.smith@company.com');

/*
 ERROR: duplicate key value violates unique constraint "employees_pkey"
  Detail: Key (emp_id)=(6) already exists.
 */

 --ex 4.2

alter table employees add column phone varchar(20) unique;

select indexname, indexdef
from pg_indexes
where tablename = 'employees' and indexname like '%phone%';

-- yes , it did , type : unique (b-tree)

--part 5
create index emp_salary_desc_idx on employees (salary desc);

SELECT emp_name, salary
FROM employees
ORDER BY salary DESC;
--it sorts data efficiently


--ex 5.2
create index proj_budget_nulls_first_idx on projects(budget NULLS FIRST);

SELECT project_name, budget
FROM projects
ORDER BY budget NULLS FIRST;

--part 6
create index emp_name_lower_idx on employees(LOWER(emp_name));
SELECT * FROM employees WHERE LOWER(emp_name) = 'john smith';


--without the index, postgres would scan the entire table and apply lower() to every name, making it slow

--ex 6.2
ALTER TABLE employees ADD COLUMN hire_date DATE;
UPDATE employees SET hire_date = '2020-01-15' WHERE emp_id = 1;
UPDATE employees SET hire_date = '2019-06-20' WHERE emp_id = 2;
UPDATE employees SET hire_date = '2021-03-10' WHERE emp_id = 3;
UPDATE employees SET hire_date = '2020-11-05' WHERE emp_id = 4;
UPDATE employees SET hire_date = '2018-08-25' WHERE emp_id = 5;

CREATE INDEX emp_hire_year_idx ON employees(EXTRACT(YEAR FROM hire_date));


SELECT emp_name, hire_date
FROM employees
WHERE EXTRACT(YEAR FROM hire_date) = 2020;

--part 7

ALTER INDEX emp_salary_idx RENAME TO employees_salary_index;

select indexname from pg_indexes where tablename = 'employees' ;

--ex 7.2

drop index emp_salary_dept_idx;

--you might want to drop an index to free up storage space and improve write performance, since indexes slow down inserts, updates, and deletes

--ex 7.3
REINDEX INDEX employees_salary_index ;

--part 8
SELECT e.emp_name, e.salary, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 50000
ORDER BY e.salary DESC;

CREATE INDEX emp_salary_filter_idx ON employees(salary) WHERE salary > 50000;

CREATE INDEX emp_dept_idx_join ON employees(dept_id);

CREATE INDEX emp_salary_desc_idx_filter
    ON employees(salary DESC)
    WHERE salary > 50000;

--ex 8.2
CREATE INDEX proj_high_budget_idx ON projects(budget)
WHERE budget > 80000;

SELECT project_name, budget
FROM projects
WHERE budget > 80000;

--a partial index is faster and smaller because it includes only the rows that match the condition (budget > 80000), instead of indexing the entire table

--ex 8.3

EXPLAIN SELECT * FROM employees WHERE salary > 52000;

/*
 if you see index scan, the index is used
if you see seq scan, postgreSQL scans the whole table instead
 */

--part 9
CREATE INDEX dept_name_hash_idx ON departments USING HASH (dept_name);

SELECT * FROM departments WHERE dept_name = 'IT';

--We use HASH only for equality comparisons


--ex 9.2
CREATE INDEX proj_name_btree_idx ON projects(project_name);

SELECT * FROM projects WHERE project_name = 'Website Redesign';

SELECT * FROM projects WHERE project_name > 'Database';

--part 10

SELECT
schemaname,
tablename,
indexname,
pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

--dept_name_hash_idx is the largest , it has 32kB

--ex 10.2
DROP INDEX IF EXISTS proj_name_hash_idx;

--ex 10.3
CREATE VIEW index_documentation AS
SELECT
tablename,
indexname,
indexdef,
'Improves salary-based queries' as purpose
FROM pg_indexes
WHERE schemaname = 'public'
AND indexname LIKE '%salary%';
SELECT * FROM index_documentation;


/*
 1. B-tree
 2. speed up queries , sort results, join tables on a column
 3. on columns that are rarely queried , on very small tables
 4. indexes are automatically updated to reflect the changes , this can add some overhead
 5. use explain or explain analyze .
 */