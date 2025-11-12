--task 2.1
CREATE VIEW employee_details as

select e.emp_name ,
       e.salary,
       d.dept_name,
       d.location
from employees e
JOIN departments d on e.dept_id = d.dept_id;


select * from employee_details;


--task 2.2

CREATE VIEW dept_statistics as
    select d.dept_name,
           count(e.emp_id),
           avg(e.salary) as employee_count,
           max(e.salary) as maximum_salary,
           min(e.salary) as minimum_salary
    from departments d
LEFT JOIN public.employees e on d.dept_id = e.dept_id
group by d.dept_name;


select * from dept_statistics
order by employee_count DESC;

--task 2.3

create view project_overview as
    select p.project_name ,
           p.budget,
           d.dept_name,
           d.location,
           count(e.emp_id)
    from projects p
JOIN departments d on p.dept_id = d.dept_id
JOIN employees e on d.dept_id = e.dept_id
GROUP BY p.project_name, p.project_name, p.budget, d.dept_name, d.location;

select * from project_overview;


--task 2.4

create view high_earners as
    select e.emp_name,
           e.salary,
           d.dept_name
from employees e
JOIN departments d on e.dept_id = d.dept_id
WHERE e.salary > 55000
;

select * from high_earners ;

-- So i see only two employees - Jane and Sarah , because they earn more than 55 grands :)

--part 3
--task 3.1
CREATE or replace VIEW employee_details as

select e.emp_name ,
       e.salary,
       d.dept_name,
       d.location,
CASE
    when e.salary > 60000 then 'High'
    when e.salary between 50000 and 60000 then 'Medium'
    else 'Low'
end as salary_level
from employees e
JOIN departments d on e.dept_id = d.dept_id;

select * from employee_details ;


-- task 3.2
ALTER VIEW high_earners
RENAME TO top_perfomers;

--task 3.3
CREATE VIEW temp_view as
    select e.emp_name,
           e.salary
from employees e
where e.salary > 50000;

select * from temp_view;

drop view temp_view;

--part 4
--ex 4.1

CREATE OR REPLACE VIEW employee_salaries as
    select e.emp_id,
           e.emp_name,
           e.dept_id,
           e.salary
from employees e

--ex 4.2

UPDATE employee_salaries
SET salary = 52000
where emp_name = 'John Smith';

select * from employees where emp_name = 'John Smith';

--yes it got updated

--ex 4.3

insert into employee_salaries(emp_id, emp_name, dept_id, salary) VALUES (6,'Alice Johnson', 102, 58000);

select * from employee_salaries
--Yes it was successful

--ex 4.4

create view it_employees as
    select e.emp_name,
           e.dept_id
from employees e
where e.dept_id = 101
WITH LOCAL CHECK OPTION;

INSERT INTO it_employees (emp_name, dept_id)
VALUES ('Bob Wilson', 103);

--it failed

--ex part 5

-- ex 5.1

create materialized view dept_summary_mv as
    select d.dept_id,
           d.dept_name,

           COALESCE (COUNT(DISTINCT e.emp_id),0) as total_employees,
           COALESCE(SUM(e.salary),0) as total_salaries,
           COALESCE(count(distinct  p.project_id),0) as total_projects,
           COALESCE(sum(p.budget),0) as total_project_budget
    from departments d
LEFT JOIN employees e on d.dept_id = e.dept_id
left join projects p on d.dept_id = p.dept_id

group by  d.dept_id, d.dept_name

with data ;


SELECT * FROM dept_summary_mv ORDER BY total_employees DESC;


--task 5.2

INSERT INTO employees (emp_id, emp_name,dept_id,salary)
VALUES (8,'Charlie Brown',101,54000);

select * from dept_summary_mv
order by total_employees desc;

refresh materialized view dept_summary_mv

--task 5.3
CREATE unique index dept_summary_mv_idx
on dept_summary_mv (dept_id);

refresh materialized view concurrently dept_summary_mv;

--concurrently let us update materialized view not blocking reading

--ex 5.4

create materialized view project_stats_mv as
    select p.project_name,
           p.budget,
           d.dept_name,
           count(e.emp_id) as count_employees
from projects p
join departments d on p.dept_id = d.dept_id
join employees e on p.dept_id = e.dept_id
group by p.budget, p.project_name, d.dept_name
with no data;

SELECT * FROM project_stats_mv;

refresh materialized view project_stats_mv;

-- so we had an error and we fixed it with refresh function

--part 6

--ex 6.1

CREATE ROLE analyst;
CREATE ROLE data_viewer LOGIN PASSWORD 'viewer123'
CREATE USER report_user WITH PASSWORD 'report456'

SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg_%';

--task 6.2

create role db_creator
with login
password 'creator789'
CREATEDB ;

create role user_manager
with login
password 'manager101'
CREATEROLE ;

CREATE role admin_user
WITH LOGIN
PASSWORD 'admin999'
SUPERUSER ;

--task 6.3
GRANT SELECT ON employees to analyst;
GRANT SELECT ON departments to analyst;
grant select on projects to analyst;

GRANT ALL PRIVILEGES on employee_details to data_viewer;

grant select,insert on employees to report_user;

--task 6.4

create role hr_team ;
create role finance_team;
create role it_team;

create user hr_user1 with password 'hr001';
create user hr_user2 with password 'hr002';
create user finance_user1 with password 'fin001';

grant hr_user1 to hr_team;
grant hr_user2 to hr_team;
grant finance_user1 to finance_team;
grant select, update on employees to hr_team;
grant select on dept_statistics to finance_team;

--task 6.5

revoke update on employees from hr_team;

revoke hr_team from hr_user2;

revoke all privileges on employee_details from data_viewer;

--task 6.6

alter role analyst with password 'analyst123'

alter role user_manager superuser

alter role analyst with password null;

alter role data_viewer connection limit 5;

--part 7
--part 7.1

create role read_only;
grant usage on schema public to read_only;
grant select on all tables in schema public to read_only;

create role junior_analyst with login password 'junior123';
create role senior_analyst with login password 'senior123';

grant read_only to junior_analyst;
grant read_only to senior_analyst;

grant insert,update on employees to senior_analyst;

--task 7.2
create role project_manager with login password 'pm123';

alter view dept_statistics owner to project_manager;

alter table projects owner to project_manager;

sql
SELECT tablename, tableowner
FROM pg_tables
WHERE schemaname = 'public';

--task 7.3

create role temp_owner with login;
create table temp_table(
    id INT
);
alter table temp_table owner to temp_owner;

reassign owned by temp_owner to postgres;

drop owned by temp_owner;

drop role temp_owner;

--task 7.4

create view hr_employee_view as
    select * from employees
where dept_id = 102;

grant select on hr_employee_view to hr_team;

create view finance_employee_view as
    select emp_id,
           emp_name,
           salary
from employees;

grant select on finance_employee_view to finance_team;


--part 8

create or replace view dept_dashboard as
    select
        d.dept_name,
        d.location,
        count(e.emp_id) as employee_count,
        round(avg(e.salary),2) as avg_salary,
        coalesce(sum(p.budget),0) as total_project_budget,
        round(coalesce(sum(p.budget),0) / nullif(count(e.emp_id),0))
from departments d
left join employees e on d.dept_id = e.dept_id
left join projects p on d.dept_id = p.dept_id
group by d.dept_id , d.dept_name, d.location;

--ex 8.2

alter table projects
add column created_date timestamp default current_timestamp;

create or replace view high_budget_projects as
    select
        p.project_name,
        p.budget,
        d.dept_name,
        p.created_date,
        case
            when p.budget > 150000 then 'critical review required'
            when p.budget > 100000 then 'management approval needed'
            else 'standard process'
        end as approval_status
    from projects p
    join public.departments d on p.dept_id = d.dept_id
    where p.budget > 75000;

--task 8.3
create role viewer_role;
grant select on all tables in schema public to viewer_role;

create role entry_role;
grant viewer_role to entry_role;
grant insert on employees,projects to entry_role;

create role analyst_role ;
grant entry_role to analyst_role;
grant update on employees ,projects to analyst_role;

create role manager_role ;
grant analyst_role to manager_role;
grant delete on employees,projects to manager_role;

create user alice with password 'alice123';
create user bob with password 'bob123';
create user charlie with password 'charlie123';

grant viewer_role to alice;
grant analyst_role to bob;
grant manager_role to charlie;