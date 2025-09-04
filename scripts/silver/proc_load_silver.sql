/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver;
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time        TIMESTAMP;
    end_time          TIMESTAMP;
    batch_start_time  TIMESTAMP;
    batch_end_time    TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '==============================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '==============================';

    RAISE NOTICE '------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------';

    -- Loading silver.crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting data into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'    -- handling upper cases and unwanted spaces
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a' 
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'              -- Normalization/Standardization
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a' 
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) x
    WHERE x.flag_last = 1; -- select most recent record per customer thus removing duplicates

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- Loading silver.crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting data into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,          -- extract category id
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,             -- extract product key
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,                                               -- standardized data and handled missing values
        prd_start_dt::DATE,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_date
                                                                        -- calculate end date as 1 day before the next start date
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- Loading silver.crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting data into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN LENGTH(sls_order_dt::TEXT) <> 8 OR sls_order_dt <= 0 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'yyyymmdd') 
        END AS sls_order_dt,
        CASE 
            WHEN LENGTH(sls_ship_dt::TEXT) <> 8 OR sls_ship_dt <= 0 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'yyyymmdd') 
        END AS sls_ship_dt,
        CASE 
            WHEN LENGTH(sls_due_dt::TEXT) <> 8 OR sls_due_dt <= 0 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'yyyymmdd') 
        END AS sls_due_dt,                                             -- handling invalid dates and type casting
        CASE 
            WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales 
        END AS sls_sales,                                              -- recalculate sales if original value is incorrect or missing
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price 
        END AS sls_price                                               -- derive price if original value is invalid
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    RAISE NOTICE '------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------';

    -- Loading silver.erp_cust_az12
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting data into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
            ELSE cid 
        END AS cid,                                                    -- remove 'NAS' prefix if present
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL 
            ELSE bdate 
        END AS bdate,                                                  -- set future birthdays to null
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'n/a' 
        END AS gen                                                     -- normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- Loading silver.erp_loc_a101
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting data into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry                                                   -- normalize and handle missing or blank country codes
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- Loading silver.erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting data into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2
    SELECT * FROM bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec', ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================';
    RAISE NOTICE 'Loading Silver Layer is completed';
    RAISE NOTICE 'Total Load Duration % msec', ROUND(EXTRACT(EPOCH FROM batch_end_time - batch_start_time) * 1000, 3);
    RAISE NOTICE '==========================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Error occurred during loading silver layer';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==============================';
END;
$$;

-- Call the procedure
CALL silver.load_silver();
