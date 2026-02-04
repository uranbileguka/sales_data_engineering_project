/*
===============================================================================
DDL Script: Create Bronze Tables (PostgreSQL)
===============================================================================
Purpose:
    Creates tables in the 'bronze' schema.
    Existing tables are dropped before creation.
===============================================================================
*/

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- =========================
-- crm_cust_info
-- =========================
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

-- =========================
-- crm_prd_info
-- =========================
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,
    prd_key       TEXT,
    prd_nm        TEXT,
    prd_cost      INT,
    prd_line      TEXT,
    prd_start_dt  TIMESTAMP,
    prd_end_dt    TIMESTAMP
);

-- =========================
-- crm_sales_details
-- =========================
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   TEXT,
    sls_prd_key   TEXT,
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);

-- =========================
-- erp_loc_info
-- =========================
DROP TABLE IF EXISTS bronze.erp_loc_info;

CREATE TABLE bronze.erp_loc_info (
    cid    TEXT,
    cntry  TEXT
);

-- =========================
-- erp_cust_info
-- =========================
DROP TABLE IF EXISTS bronze.erp_cust_info;

CREATE TABLE bronze.erp_cust_info (
    cid    TEXT,
    bdate  DATE,
    gen    TEXT
);

-- =========================
-- erp_px_cat_info
-- =========================
DROP TABLE IF EXISTS bronze.erp_px_cat_info;

CREATE TABLE bronze.erp_px_cat_info (
    id           TEXT,
    cat          TEXT,
    subcat       TEXT,
    maintenance  TEXT
);

-- =========================
-- marketing_salesperson
-- =========================
DROP TABLE IF EXISTS bronze.marketing_salesperson;
CREATE TABLE bronze.marketing_salesperson (
    salesperson_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT,
    email TEXT
);



-- =========================
-- marketing_salesperson_sales
-- =========================
DROP TABLE IF EXISTS bronze.marketing_salesperson_sales;
CREATE TABLE bronze.marketing_salesperson_sales (
    salesperson_id TEXT,
    sls_ord_num TEXT
);


-- =========================
-- marketing_discount_info
-- =========================
DROP TABLE IF EXISTS bronze.marketing_discount_info;
CREATE TABLE bronze.marketing_discount_info (
    discount_id TEXT,
    description TEXT,
    percent INT,
    active TEXT
);


-- =========================
-- marketing_sales_discount
-- =========================
DROP TABLE IF EXISTS bronze.marketing_sales_discount;
CREATE TABLE bronze.marketing_sales_discount (
    discount_id TEXT,
    sls_ord_num TEXT
);


