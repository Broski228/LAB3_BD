--Task 1.1

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT CHECK(age BETWEEN 18 AND 65),
    salary NUMERIC CHECK(salary>0)
);
--Task 1.2
CREATE TABLE products_catalog(
    product_id INT PRIMARY KEY ,
    product_name VARCHAR(50),
    regular_price NUMERIC,
    discount_price NUMERIC,
    CONSTRAINT valid_discount CHECK(regular_price>0 AND discount_price >0 AND discount_price<regular_price)
);

--Task 1.3

CREATE TABLE bookings (
    booking_id INT PRIMARY KEY ,
    check_in_date DATE,
    check_out_date DATE CHECK(check_out_date > check_in_date),
    num_guests INT CHECK (num_guests BETWEEN 1 AND 10)
);


--Task 1.4

INSERT INTO employees (employee_id, first_name, last_name, age, salary)
VALUES (1, 'Alinur', 'Nurlan', 18, 777000);
INSERT INTO employees(employee_id, first_name, last_name, age, salary)
VALUES (2,'Kamila', 'Balabatyr',19, 888000);

INSERT INTO products_catalog (product_id, product_name, regular_price, discount_price)
VALUES (1,'banana', 99, 89 );
INSERT INTO  products_catalog(product_id, product_name, regular_price, discount_price)
VALUES (2,'apple',79,69);

INSERT INTO bookings (booking_id, check_in_date, check_out_date, num_guests)
VALUES (1,'2022-10-25','2022-10-29',3);
INSERT INTO bookings(booking_id, check_in_date, check_out_date, num_guests)
VALUES (2,'2023-9-5','2023-9-14',1);

--Invalid data time :D
INSERT INTO employees (employee_id, first_name, last_name, age, salary)
VALUES (3, 'Whats', 'Wrong', 71, -10);
--[2025-10-12 19:29:41] [23514] ERROR: new row for relation "employees" violates check constraint "employees_age_check"
--[2025-10-12 19:29:41] Detail: Failing row contains (3, Whats, Wrong, 71, -10).

INSERT INTO products_catalog (product_id, product_name, regular_price, discount_price)
VALUES (5,'banana', -3, -1 );

--[2025-10-12 19:30:58] [23514] ERROR: new row for relation "products_catalog" violates check constraint "valid_discount"
--[2025-10-12 19:30:58] Detail: Failing row contains (5, banana, -3, -1).

INSERT INTO bookings(booking_id, check_in_date, check_out_date, num_guests)
VALUES (10,'2023-9-5','2023-9-4',11);
--[2025-10-12 19:31:55] [23514] ERROR: new row for relation "bookings" violates check constraint "bookings_check"
--[2025-10-12 19:31:55] Detail: Failing row contains (10, 2023-09-05, 2023-09-04, 11).

--You could notice , that i wrote wrong datas in each tables , so the constraint didnt prove them and gave us ERRORS.

--PART 2

--Task 2.1

CREATE TABLE customers (
    customer_id INT NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    registration_date DATE NOT NULL
);

--Task 2.2
CREATE TABLE inventory (
    item_id INT PRIMARY KEY NOT NULL,
    item_name VARCHAR(50) NOT NULL,
    quantity INT NOT NULL CHECK (quantity>=0),
    unit_price NUMERIC NOT NULL CHECK (unit_price>0),
    last_updated TIMESTAMP NOT NULL

);

--Task 2.3
--1
INSERT INTO customers (customer_id, email, phone, registration_date)
VALUES (1, 'dksak@kbtu.kz', 'iphone', '2025-10-25');

INSERT INTO inventory (item_id, item_name, quantity, unit_price, last_updated)
VALUES (1, 'lolipop', 3, 99, '2025-10-25 14:30:00');

--2
INSERT INTO customers (customer_id, email, phone, registration_date)
VALUES (NULL,'dksak@kbtu.kz', 'iphone', '2025-10-25');
--[2025-10-12 20:52:08] [23502] ERROR: null value in column "customer_id" of relation "customers" violates not-null constraint

--3
-- Let's check it on "bookings" table!
INSERT INTO bookings(booking_id, check_in_date, check_out_date, num_guests)
VALUES (3, NULL, NULL ,NULL);

--PART 3
--Task 3.1
CREATE TABLE users (
    user_id INT PRIMARY KEY ,
    username VARCHAR(50) ,
    email VARCHAR(50) ,
    created_at TIMESTAMP,
    UNIQUE(username),
    UNIQUE(email)
);
--Task 3.2
CREATE TABLE course_enrollments (
    enrollment_id INT,
    student_id INT  ,
    course_code VARCHAR(50),
    semester VARCHAR(50),
    UNIQUE(student_id,course_code,semester)
);

--Task 3.3
ALTER TABLE users
ADD CONSTRAINT unique_username UNIQUE (username);

ALTER TABLE users
ADD CONSTRAINT unique_email UNIQUE(email);

INSERT INTO users(user_id, username, email, created_at)
VALUES (1, 'gagarin' , 'kiki@kbtu.kz', '2023-10-9');

INSERT INTO users(user_id, username, email, created_at)
VALUES (2, 'gagarin' , 'kiki@kbtu.kz', '2023-11-9');
--[2025-10-12 21:29:29] [23505] ERROR: duplicate key value violates unique constraint "users_username_key"

--PART 4
--Task 4.1
CREATE TABLE departments (
    dept_id INT PRIMARY KEY ,
    dept_name VARCHAR(50) NOT NULL,
    location VARCHAR(50)
);

INSERT INTO departments(dept_id, dept_name, location)
VALUES (1,'mary','aktau');

INSERT INTO departments(dept_id, dept_name, location)
VALUES (1,NULL,'astana');
--[2025-10-12 21:37:42] [23502] ERROR: null value in column "dept_name" of relation "departments" violates not-null constraint

INSERT INTO departments(dept_id, dept_name, location)
VALUES (1,'GARY','astana');
--[2025-10-12 21:38:16] Detail: Key (dept_id)=(1) already exists.

--Task 4.2
CREATE TABLE student_courses (
    student_id INT,
    course_id INT,
    enrollment_date DATE,
    grade VARCHAR(50),
    PRIMARY KEY (student_id,course_id)
)
--Task 4.3
--The task in a different file

--PART 5
--Task 5.1
CREATE TABLE employees_dept (
    emp_id INT PRIMARY KEY ,
    emp_name VARCHAR(50) NOT NULL,
    dept_id INT REFERENCES departments (dept_id),
    hire_date DATE
);

--Task 5.2
CREATE TABLE authors (
    author_id INT PRIMARY KEY ,
    author_name VARCHAR(50) NOT NULL,
    country VARCHAR(50)
);
CREATE TABLE publishers (
    publisher_id INT PRIMARY KEY ,
    publisher_name VARCHAR(50) NOT NULL ,
    city VARCHAR(50)
);
CREATE TABLE books (
    book_id INT PRIMARY KEY ,
    title VARCHAR(50) NOT NULL ,
    author_id INT REFERENCES authors(author_id),
    publisher_id INT REFERENCES publishers(publisher_id),
    publication_year INT ,
    isbn VARCHAR(50) UNIQUE
);

--Task 5.3

CREATE TABLE categories (
    category_id INT PRIMARY KEY ,
    category_name VARCHAR(50) NOT NULL
);
CREATE TABLE products_fk (
    product_id INT PRIMARY KEY ,
    product_name VARCHAR(50) NOT NULL,
    category_id INT REFERENCES categories(category_id) ON DELETE RESTRICT
);
CREATE TABLE orders (
    order_id INT PRIMARY KEY ,
    order_date DATE NOT NULL
);
CREATE TABLE order_items (
    item_id INT PRIMARY KEY ,
    order_id INT REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INT REFERENCES products_fk(product_id),
    quantity INT CHECK (quantity>0)
);
INSERT INTO categories (category_id, category_name)
VALUES (1,'fruits');
INSERT INTO products_fk(product_id, product_name, category_id)
VALUES (10,'apple',1);


DELETE FROM categories WHERE category_id = 1;
--[2025-10-13 12:11:55] [23503] ERROR: update or delete on table "categories" violates foreign key constraint "products_fk_category_id_fkey" on table "products_fk"
--[2025-10-13 12:11:55] Detail: Key (category_id)=(1) is still referenced from table "products_fk".
-- Here it cannot delete the parents data , its data only .
INSERT INTO orders (order_id, order_date)
VALUES (1, '2025-10-12');

INSERT INTO order_items (item_id, order_id, product_id, quantity)
VALUES (1, 1, 10, 5);

DELETE FROM order_items WHERE order_id = 1 ;
-- Here it deletes all the data , and from the parents table also !

--PART 6
--Task 6.1

CREATE TABLE customers2(
    customer_id INT PRIMARY KEY ,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone INT NOT NULL,
    registration_date DATE,
    UNIQUE (email)
);

CREATE TABLE products2 (
    product_id INT PRIMARY KEY ,
    name VARCHAR(50) NOT NULL,
    description VARCHAR(50) NOT NULL,
    price NUMERIC CHECK (price>0) NOT NULL,
    stock_quantity INT CHECK(stock_quantity>0) NOT NULL
);

CREATE TABLE orders2 (
    order_id INT PRIMARY KEY ,
    customer_id INT NOT NULL REFERENCES customers2(customer_id),
    order_date DATE,
    total_amount INT NOT NULL,
    status VARCHAR(20) CHECK (status = 'pending' OR status = 'processing' OR status = 'shipped' OR status = 'delivered' OR status = 'cancelled')
);
CREATE TABLE order_details (
    order_detail_id INT PRIMARY KEY ,
    order_id INT REFERENCES orders2(order_id),
    product_id INT REFERENCES products2(product_id),
    quantity INT NOT NULL CHECK (quantity>0),
    unit_price NUMERIC NOT NULL
);

INSERT INTO customers2 (customer_id, name, email, phone, registration_date)
VALUES (1,'joni','a@k.kz',7777,'2025-10-1');
INSERT INTO customers2 (customer_id, name, email, phone, registration_date)
VALUES (2,'joni1','b@k.kz',7771,'2025-10-2');
INSERT INTO customers2 (customer_id, name, email, phone, registration_date)
VALUES (3,'joni2','c@k.kz',7772,'2025-10-3');
INSERT INTO customers2 (customer_id, name, email, phone, registration_date)
VALUES (4,'joni3','d@k.kz',7773,'2025-10-4');
INSERT INTO customers2 (customer_id, name, email, phone, registration_date)
VALUES (5,'joni4','e@k.kz',7774,'2025-10-5');

INSERT INTO products2(product_id, name, description, price, stock_quantity)
VALUES (1,'gigi','wow',11,1);
INSERT INTO products2(product_id, name, description, price, stock_quantity)
VALUES (2,'gigi1','wow1',15,8);
INSERT INTO products2(product_id, name, description, price, stock_quantity)
VALUES (3,'gigi2','wow2',71,9);
INSERT INTO products2(product_id, name, description, price, stock_quantity)
VALUES (4,'gigi3','wow3',41,3);
INSERT INTO products2(product_id, name, description, price, stock_quantity)
VALUES (5,'gigi4','wow4',13,2);

INSERT INTO orders2 (order_id, customer_id, order_date, total_amount, status)
VALUES (1,1,'2023-10-8',3,'pending');
INSERT INTO orders2 (order_id, customer_id, order_date, total_amount, status)
VALUES (2,2,'2023-10-7',6,'processing');
INSERT INTO orders2 (order_id, customer_id, order_date, total_amount, status)
VALUES (3,4,'2023-10-9',3,'shipped');
INSERT INTO orders2 (order_id, customer_id, order_date, total_amount, status)
VALUES (4,5,'2023-11-8',9,'delivered');
INSERT INTO orders2 (order_id, customer_id, order_date, total_amount, status)
VALUES (5,3,'2023-12-8',3,'cancelled');

INSERT INTO order_details(order_detail_id, order_id, product_id, quantity, unit_price)
VALUES (1,3,4, 4, 99);
INSERT INTO order_details(order_detail_id, order_id, product_id, quantity, unit_price)
VALUES (2,1,5, 6, 99);
INSERT INTO order_details(order_detail_id, order_id, product_id, quantity, unit_price)
VALUES (3,2,2, 2, 99);
INSERT INTO order_details(order_detail_id, order_id, product_id, quantity, unit_price)
VALUES (4,4,3, 4, 99);
INSERT INTO order_details(order_detail_id, order_id, product_id, quantity, unit_price)
VALUES (5,5,1, 1, 99);