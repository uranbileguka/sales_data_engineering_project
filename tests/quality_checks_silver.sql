/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;

-- ====================================================================
-- Checking 'silver.marketing_salesperson'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
    salesperson_id,
    COUNT(*)
FROM silver.marketing_salesperson
GROUP BY salesperson_id
HAVING COUNT(*) > 1 OR salesperson_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
    salesperson_id,
    name,
    region,
    email
FROM silver.marketing_salesperson
WHERE name != TRIM(name)
   OR region != TRIM(region)
   OR email != TRIM(email);

-- Data Standardization & Consistency
SELECT DISTINCT
    region
FROM silver.marketing_salesperson
ORDER BY region;

-- ====================================================================
-- Checking 'silver.marketing_discount_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
    discount_id,
    COUNT(*)
FROM silver.marketing_discount_info
GROUP BY discount_id
HAVING COUNT(*) > 1 OR discount_id IS NULL;

-- Check for Invalid Discount Percent Range
-- Expectation: No Results
SELECT
    discount_id,
    percent
FROM silver.marketing_discount_info
WHERE percent < 0 OR percent > 100 OR percent IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT
    active
FROM silver.marketing_discount_info;

-- ====================================================================
-- Checking 'silver.marketing_salesperson_sales'
-- ====================================================================
-- Referential Integrity: salesperson_id must exist in master table
-- Expectation: No Results
SELECT
    mss.salesperson_id,
    mss.sls_ord_num
FROM silver.marketing_salesperson_sales mss
LEFT JOIN silver.marketing_salesperson ms
ON ms.salesperson_id = mss.salesperson_id
WHERE ms.salesperson_id IS NULL;

-- Referential Integrity: order number must exist in sales details
-- Expectation: No Results
SELECT
    mss.salesperson_id,
    mss.sls_ord_num
FROM silver.marketing_salesperson_sales mss
LEFT JOIN silver.crm_sales_details csd
ON csd.sls_ord_num = mss.sls_ord_num
WHERE csd.sls_ord_num IS NULL;

-- ====================================================================
-- Checking 'silver.marketing_sales_discount'
-- ====================================================================
-- Referential Integrity: discount_id must exist in discount master table
-- Expectation: No Results
SELECT
    msd.discount_id,
    msd.sls_ord_num
FROM silver.marketing_sales_discount msd
LEFT JOIN silver.marketing_discount_info mdi
ON mdi.discount_id = msd.discount_id
WHERE mdi.discount_id IS NULL;

-- Referential Integrity: order number must exist in sales details
-- Expectation: No Results
SELECT
    msd.discount_id,
    msd.sls_ord_num
FROM silver.marketing_sales_discount msd
LEFT JOIN silver.crm_sales_details csd
ON csd.sls_ord_num = msd.sls_ord_num
WHERE csd.sls_ord_num IS NULL;
