/* =====================================================
   CUSTOMER REVENUE INTELLIGENCE PLATFORM
   
   Author: Dev
   
   Technologies:
   
   - AWS S3
   - Snowflake
   - Snowpipe
   - Streams
   - Tasks
   - SQL Analytics

   NOTE:
   Credentials, ARNs, bucket names and secrets
   have been replaced with placeholders.
===================================================== */


/* =====================================================
   DATABASE SETUP
===================================================== */

CREATE DATABASE customer_revenue_intelligence;

USE DATABASE customer_revenue_intelligence;


/* =====================================================
   STORAGE INTEGRATION
===================================================== */

CREATE OR REPLACE STORAGE INTEGRATION customer_s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'
STORAGE_ALLOWED_LOCATIONS = (
's3://<YOUR_BUCKET_NAME>/'
);


/* =====================================================
   EXTERNAL STAGE
===================================================== */

CREATE OR REPLACE STAGE customer_stage
URL='s3://<YOUR_BUCKET_NAME>/'
STORAGE_INTEGRATION = customer_s3_int;


/* =====================================================
   SCHEMA CREATION
===================================================== */

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;

/* =====================================================
   RAW LAYER TABLES
===================================================== */

CREATE OR REPLACE TABLE raw.customers (
    customer_id INTEGER,
    customer_name STRING,
    city STRING,
    signup_date DATE,
    customer_segment STRING
);

CREATE OR REPLACE TABLE raw.products (
    product_id INTEGER,
    product_name STRING,
    category STRING,
    cost_price NUMBER(10,2),
    selling_price NUMBER(10,2)
);


CREATE OR REPLACE TABLE raw.orders (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMBER(10,2),
    discount_pct NUMBER(5,2),
    payment_method STRING,
    order_status STRING,
    city STRING,
    sales_channel STRING,
    customer_rating INTEGER,
    order_date DATE
);

CREATE OR REPLACE TABLE raw.customer_events (
    event_data VARIANT
);


/* =====================================================
   FILE FORMATS
===================================================== */

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
DATE_FORMAT='MM/DD/YYYY';

CREATE OR REPLACE FILE FORMAT json_format
TYPE = JSON;


/* =====================================================
   DATA LOADING
===================================================== */

-- COPY INTO statements here
COPY INTO raw.customers
FROM @public.customer_stage/customer_DATA.csv
FILE_FORMAT = csv_format;

COPY INTO raw.products
FROM @public.customer_stage/product.csv
FILE_FORMAT = csv_format
FORCE = TRUE;

COPY INTO raw.orders
FROM @public.customer_stage/orders.csv
FILE_FORMAT = csv_format
FORCE = TRUE;

COPY INTO raw.customer_events
FROM @public.customer_stage/customer_events.csv
FILE_FORMAT = json_format
FORCE = TRUE;

/* =====================================================
   STAGING LAYER
===================================================== */

-- staging.customers
CREATE OR REPLACE TABLE staging.customers AS
SELECT DISTINCT *
FROM raw.customers;

-- staging.products
CREATE OR REPLACE TABLE staging.products AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    cost_price,
    selling_price
FROM raw.products;

-- staging.orders
CREATE OR REPLACE TABLE staging.orders AS
SELECT DISTINCT *
FROM raw.orders;

-- staging.customer_events

CREATE OR REPLACE TABLE staging.customer_events AS
SELECT
    value:event_id::INTEGER AS event_id,
    value:customer_id::INTEGER AS customer_id,
    value:event_type::STRING AS event_type,
    value:event_timestamp::TIMESTAMP AS event_timestamp,
    value:device_type::STRING AS device_type,
    value:os::STRING AS os,
    value:product_id::INTEGER AS product_id,
    value:session_duration::INTEGER AS session_duration
FROM raw.customer_events,
LATERAL FLATTEN(input => event_data);


/* =====================================================
   MARTS LAYER
===================================================== */

-- customer_revenue_fact
CREATE OR REPLACE TABLE marts.customer_revenue_fact AS
SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(quantity * unit_price * (1-discount_pct/100)) AS total_revenue
FROM staging.orders
GROUP BY customer_id;

-- product_performance_fact
CREATE OR REPLACE TABLE marts.product_performance_fact AS
SELECT
    product_id,
    SUM(quantity) AS units_sold,
    SUM(quantity * unit_price) AS revenue
FROM staging.orders
GROUP BY product_id;

-- monthly_revenue_fact
CREATE OR REPLACE TABLE marts.monthly_revenue_fact AS
SELECT
    DATE_TRUNC('MONTH',order_date) AS month,
    SUM(quantity * unit_price * (1-discount_pct/100)) revenue
FROM staging.orders
GROUP BY month;

-- customer_engagement_fact

CREATE OR REPLACE TABLE marts.customer_engagement_fact AS
SELECT
    customer_id,
    COUNT(*) total_events
FROM staging.customer_events
GROUP BY customer_id;


/* =====================================================
   ANALYTICS QUERIES
===================================================== */

SELECT *
FROM marts.product_performance_fact
ORDER BY revenue DESC
LIMIT 10;

SELECT
    city,
    SUM(quantity * unit_price) revenue
FROM staging.orders
GROUP BY city
ORDER BY revenue DESC;

SELECT *
FROM marts.product_performance_fact
ORDER BY revenue DESC
LIMIT 10;

SELECT 
    city,
    SUM(quantity * unit_price * (1-discount_pct/100)) AS revenue
FROM staging.orders
GROUP BY city
ORDER BY revenue DESC;

--customer Ranking based on total revenue
SELECT customer_id,total_revenue,RANK() OVER(ORDER BY total_revenue DESC) AS customer_rank
FROM marts.customer_revenue_fact;

--City Vise total revenue
SELECT * FROM (
    SELECT
    c.city,crf.total_revenue,ROW_NUMBER() OVER(PARTITION BY c.city  ORDER BY crf.total_revenue DESC
        ) AS rn FROM marts.customer_revenue_fact crf
        join staging.customers c
        on crf.customer_id=c.customer_id
)WHERE rn <= 10;


-- Highest Revenue Generating Customers
SELECT

    c.customer_id,
    c.customer_name,
    crf.total_revenue,
    RANK() OVER(
        ORDER BY crf.total_revenue DESC
    ) AS revenue_rank
FROM marts.customer_revenue_fact crf
join staging.customers c
on crf.customer_id=c.customer_id ;

-- Business Monthly growth
SELECT
     month,
     revenue,LAG(revenue) OVER(ORDER BY month) AS previous_month,revenue- LAG(revenue)OVER(ORDER BY month)AS growth
FROM marts.monthly_revenue_fact;


/* =====================================================
   SNOWPIPE
===================================================== */

-- orders_pipe
CREATE OR REPLACE PIPE orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.orders
FROM @public.customer_stage
PATTERN='.*orders.*'
FILE_FORMAT = csv_format;

-- customers_pipe
CREATE OR REPLACE PIPE customers_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.customers
FROM @public.customer_stage
PATTERN='.*customers.*'
FILE_FORMAT = csv_format;

-- products_pipe

CREATE OR REPLACE PIPE products_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.products
FROM @public.customer_stage
PATTERN='.*product.*'
FILE_FORMAT = csv_format;
-- events_pipe
CREATE OR REPLACE PIPE events_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.customer_events
FROM @public.customer_stage
PATTERN='.*customer_events.*'
FILE_FORMAT = json_format;

/* =====================================================
   STREAMS
===================================================== */




-- orders_stream
CREATE OR REPLACE STREAM orders_stream
ON TABLE raw.orders;

-- customers_stream
CREATE OR REPLACE STREAM customers_stream
ON TABLE raw.customers;

-- products_stream
CREATE OR REPLACE STREAM products_stream
ON TABLE raw.products;

-- events_stream

CREATE OR REPLACE STREAM events_stream
ON TABLE raw.customer_events;


/* =====================================================
   TASKS
===================================================== */

-- orders_task 
CREATE OR REPLACE TASK orders_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
MERGE INTO staging.orders s
USING orders_stream o
ON s.order_id = o.order_id
WHEN MATCHED THEN
UPDATE SET
    customer_id = o.customer_id,
    product_id = o.product_id,
    quantity = o.quantity,
    unit_price = o.unit_price,
    discount_pct = o.discount_pct,
    payment_method = o.payment_method,
    order_status = o.order_status,
    city = o.city,
    sales_channel = o.sales_channel,
    customer_rating = o.customer_rating,
    order_date = o.order_date

WHEN NOT MATCHED THEN
INSERT (
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    payment_method,
    order_status,
    city,
    sales_channel,
    customer_rating,
    order_date
)
VALUES (
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.payment_method,
    o.order_status,
    o.city,
    o.sales_channel,
    o.customer_rating,
    o.order_date
);

-- customers_task 

CREATE OR REPLACE TASK customers_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
MERGE INTO staging.customers s
USING customers_stream c
ON s.customer_id = c.customer_id
WHEN MATCHED THEN
UPDATE SET
    customer_name = c.customer_name,
    city = c.city,
    signup_date = c.signup_date,
    customer_segment = c.customer_segment
WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    customer_name,
    city,
    signup_date,
    customer_segment
)
VALUES (
    c.customer_id,
    c.customer_name,
    c.city,
    c.signup_date,
    c.customer_segment
);

-- products_task 
CREATE OR REPLACE TASK products_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
MERGE INTO staging.products s
USING products_stream p
ON s.product_id = p.product_id
WHEN MATCHED THEN
UPDATE SET
    product_name = p.product_name,
    category = p.category,
    cost_price = p.cost_price,
    selling_price = p.selling_price
WHEN NOT MATCHED THEN
INSERT (
    product_id,
    product_name,
    category,
    cost_price,
    selling_price
)
VALUES (
    p.product_id,
    p.product_name,
    p.category,
    p.cost_price,
    p.selling_price
);
-- events_task 

CREATE OR REPLACE TASK events_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
INSERT INTO staging.customer_events
SELECT *
FROM events_stream;

/* =====================================================
   END OF PROJECT
===================================================== */