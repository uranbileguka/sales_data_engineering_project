/*
===============================================================================
Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Purpose:
    Loads curated Silver tables from raw Bronze tables by applying cleansing,
    standardization, and basic transformation rules.

Actions:
    - Truncates Silver tables to support repeatable runs.
    - Inserts cleaned/transformed data from Bronze into Silver.

Parameters:
    None.

Run:
    CALL silver.load_silver();
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- silver.crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    -- silver.crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key FROM 7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt::date AS prd_start_dt,
        (
          LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
          - INTERVAL '1 day'
        )::date AS prd_end_dt
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    -- silver.crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN (COALESCE(sls_sales, 0)::numeric / NULLIF(sls_quantity, 0))::int
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';


    -- silver.erp_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_info';
    TRUNCATE TABLE silver.erp_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_info';
    INSERT INTO silver.erp_cust_info (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    -- silver.erp_loc_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_info';
    TRUNCATE TABLE silver.erp_loc_info;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_info';
    INSERT INTO silver.erp_loc_info (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    -- silver.erp_px_cat_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_info';
    TRUNCATE TABLE silver.erp_px_cat_info;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_info';
    INSERT INTO silver.erp_px_cat_info (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading Marketing Tables';
    RAISE NOTICE '------------------------------------------------';


    -- silver.marketing_salesperson
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.marketing_salesperson';
    TRUNCATE TABLE silver.marketing_salesperson;

    RAISE NOTICE '>> Inserting Data Into: silver.marketing_salesperson';
    INSERT INTO silver.marketing_salesperson ( salesperson_id, name, region, email)
    SELECT salesperson_id, name, region, email
    FROM bronze.marketing_salesperson;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';

    -- silver.marketing_salesperson_sales
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.marketing_salesperson_sales';
    TRUNCATE TABLE silver.marketing_salesperson_sales;

    RAISE NOTICE '>> Inserting Data Into: silver.marketing_salesperson_sales';
    INSERT INTO silver.marketing_salesperson_sales (salesperson_id, sls_ord_num)
    SELECT salesperson_id, sls_ord_num
    FROM bronze.marketing_salesperson_sales;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';

    -- silver.marketing_discount_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.marketing_discount_info';
    TRUNCATE TABLE silver.marketing_discount_info;

    RAISE NOTICE '>> Inserting Data Into: silver.marketing_discount_info';
    INSERT INTO silver.marketing_discount_info (discount_id, description, percent, active)
    SELECT discount_id, description, percent, active
    FROM bronze.marketing_discount_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';

    -- silver.marketing_sales_discount
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.marketing_sales_discount';
    TRUNCATE TABLE silver.marketing_sales_discount;

    RAISE NOTICE '>> Inserting Data Into: silver.marketing_sales_discount';
    INSERT INTO silver.marketing_sales_discount (discount_id, sls_ord_num)
    SELECT discount_id, sls_ord_num
    FROM bronze.marketing_sales_discount;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time))::int;
    RAISE NOTICE '>> -------------';


    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::int;
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
    RAISE NOTICE '==========================================';
    RAISE;
END;
$$;
