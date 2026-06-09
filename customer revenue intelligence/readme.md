# 🚀 Customer Revenue Intelligence Platform

An end-to-end Data Engineering and Analytics project built using **AWS S3** and **Snowflake** to automate data ingestion, transformation, and business analytics.

## 📌 Project Overview

This project simulates a real-world customer revenue analytics platform where customer, product, order, and event data are ingested from AWS S3 into Snowflake and transformed through a modern data warehouse architecture.

The platform follows a multi-layer architecture:

```text
AWS S3
   ↓
Snowpipe
   ↓
RAW Layer
   ↓
Streams
   ↓
Tasks
   ↓
STAGING Layer
   ↓
MARTS Layer
   ↓
Business Analytics
```

---

## 🛠 Tech Stack

* AWS S3
* Snowflake
* Snowpipe
* Streams
* Tasks
* SQL
* Window Functions
* Data Warehousing

---

## 📂 Data Sources

### Customers

* Customer ID
* Customer Name
* City
* Signup Date
* Customer Segment

### Products

* Product ID
* Product Name
* Category
* Cost Price
* Selling Price

### Orders

* Order ID
* Customer ID
* Product ID
* Quantity
* Unit Price
* Discount
* Payment Method
* Order Status
* Sales Channel
* Customer Rating
* Order Date

### Customer Events (JSON)

* Event ID
* Customer ID
* Event Type
* Device Type
* Session Duration
* Timestamp

---

## 🏗 Data Architecture

### RAW Layer (Bronze)

Stores original data loaded from AWS S3.

Tables:

* raw.customers
* raw.products
* raw.orders
* raw.customer_events

### STAGING Layer (Silver)

Data cleansing and transformation layer.

Tables:

* staging.customers
* staging.products
* staging.orders
* staging.customer_events

### MARTS Layer (Gold)

Business-ready analytics layer.

Tables:

* customer_revenue_fact
* product_performance_fact
* monthly_revenue_fact
* customer_engagement_fact

---

## ⚡ Automation Features

### Snowpipe

Automatically ingests files arriving in AWS S3.

### Streams

Tracks new data arriving in RAW tables.

### Tasks

Automates incremental loading from RAW to STAGING.

### MERGE-Based Upserts

Prevents duplicate records during incremental processing.

---

## 📊 Analytics Implemented

### Revenue Analytics

* Top Revenue Customers
* Revenue by City
* Monthly Revenue Trends
* Revenue Growth Analysis

### Product Analytics

* Top Selling Products
* Product Revenue Analysis

### Customer Analytics

* Customer Ranking
* Customer Engagement Metrics

### Advanced SQL

* RANK()
* ROW_NUMBER()
* LAG()
* PARTITION BY
* Window Functions
---

## 🎯 Key Learnings

* Data Warehouse Design
* AWS S3 Integration
* Snowflake Architecture
* Automated Data Pipelines
* Change Data Capture (CDC)
* Incremental Processing
* SQL Analytics
* Business Intelligence

---

## 🔮 Future Improvements

* Power BI Dashboard
* Tableau Dashboard
* Snowflake Dynamic Tables
* Data Quality Monitoring
* CI/CD Integration
* dbt Transformations

---

## 👨‍💻 Author

**Dev**

Aspiring Data Scientist & Data Engineer passionate about building scalable data platforms and analytics solutions.
