/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Purpose:
    Defines and creates tables in the 'silver' schema.
    Existing tables are dropped and recreated to refresh the Silver layer
    structure derived from Bronze tables.
===============================================================================
*/


CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            TEXT,
    cst_firstname      TEXT,
    cst_lastname       TEXT,
    cst_marital_status TEXT,
    cst_gndr           TEXT,
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id         INT,
    cat_id         TEXT,
    prd_key        TEXT,
    prd_nm         TEXT,
    prd_cost       INT,
    prd_line       TEXT,
    prd_start_dt   DATE,
    prd_end_dt     DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num   TEXT,
    sls_prd_key   TEXT,
    sls_cust_id   INT,
    sls_order_dt  DATE,
    sls_ship_dt   DATE,
    sls_due_dt    DATE,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.erp_cust_info;
CREATE TABLE silver.erp_cust_info (
    cid            TEXT,
    bdate          DATE,
    gen            TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.erp_loc_info;
CREATE TABLE silver.erp_loc_info (
    cid            TEXT,
    cntry          TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS silver.erp_px_cat_info;
CREATE TABLE silver.erp_px_cat_info (
    id            TEXT,
    cat           TEXT,
    subcat        TEXT,
    maintenance   TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.marketing_salesperson;
CREATE TABLE silver.marketing_salesperson (
    salesperson_id TEXT,
    name           TEXT,
    region         TEXT,
    email          TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.marketing_salesperson_sales;
CREATE TABLE silver.marketing_salesperson_sales (
    salesperson_id TEXT,
    sls_ord_num     TEXT,
    dwh_create_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.marketing_discount_info;
CREATE TABLE silver.marketing_discount_info (
    discount_id    TEXT,
    description    TEXT,
    percent        INT,
    active         TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.marketing_sales_discount;
CREATE TABLE silver.marketing_sales_discount (
    discount_id    TEXT,
    sls_ord_num     TEXT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
