/*
 ===============================================================================
 DDL: Create Gold Views (PostgreSQL)
 ===============================================================================
 Purpose:
 Create Gold-layer views that serve as the business-ready star schema
 (final dimensions and facts). These views transform and join Silver-layer
 data into clean datasets for analytics/reporting.
 ===============================================================================
 */
-- =============================================================================
-- Dimension View: gold.dim_customers
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT ROW_NUMBER() OVER (
        ORDER BY ci.cst_id
    ) AS customer_key,
    -- surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr -- CRM is primary for gender
        ELSE COALESCE(ca.gen, 'n/a') -- fallback to ERP
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_info ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_info la ON ci.cst_key = la.cid;
-- =============================================================================
-- Dimension View: gold.dim_products
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER (
        ORDER BY pn.prd_start_dt,
            pn.prd_key
    ) AS product_key,
    -- surrogate key
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_info pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;
-- keep only current (non-historical) products
-- =============================================================================
-- Table: silver.dim_salesperson
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_salesperson;
CREATE VIEW gold.dim_salesperson AS
SELECT ROW_NUMBER() OVER (
        ORDER BY salesperson_id
    ) AS salesperson_key,
    salesperson_id,
    name AS salesperson_name,
    region,
    email
FROM silver.marketing_salesperson;
-- =============================================================================
-- Table: silver.dim_discount
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_discount;
CREATE VIEW gold.dim_discount AS(
    SELECT ROW_NUMBER() OVER (
            ORDER BY discount_id
        ) AS discount_key,
        discount_id as discount_id,
        description as discount_description,
        percent as discount_percent,
        active as discount_active
    FROM silver.marketing_discount_info
);
-- =============================================================================
-- Fact View: gold.fact_sales
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price,
    sp.salesperson_key AS salesperson_key,
    dd.discount_key AS discount_key
FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
    LEFT JOIN silver.marketing_salesperson_sales msp ON sd.sls_ord_num = msp.sls_ord_num
    LEFT JOIN gold.dim_salesperson sp ON msp.salesperson_id = sp.salesperson_id
    LEFT JOIN silver.marketing_sales_discount msd ON sd.sls_ord_num = msd.sls_ord_num
    LEFT JOIN gold.dim_discount dd ON msd.discount_id = dd.discount_id;