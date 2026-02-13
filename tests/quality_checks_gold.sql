/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_salesperson'
-- ====================================================================
-- Check for Uniqueness of Salesperson Key in gold.dim_salesperson
-- Expectation: No results 
SELECT 
    salesperson_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_salesperson
GROUP BY salesperson_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_discount'
-- ====================================================================
-- Check for Uniqueness of Discount Key in gold.dim_discount
-- Expectation: No results 
SELECT 
    discount_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_discount
GROUP BY discount_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;

-- Check the data model connectivity between fact_sales and salesperson/discount dimensions
-- Expectation: No results (for non-null foreign keys)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_salesperson s
ON s.salesperson_key = f.salesperson_key
LEFT JOIN gold.dim_discount d
ON d.discount_key = f.discount_key
WHERE (f.salesperson_key IS NOT NULL AND s.salesperson_key IS NULL)
   OR (f.discount_key IS NOT NULL AND d.discount_key IS NULL);
