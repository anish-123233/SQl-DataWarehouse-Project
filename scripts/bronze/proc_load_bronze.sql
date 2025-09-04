/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY FROM` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC CALL bronze.oad_bronze
===============================================================================
*/

-- Create or replace procedure to load Bronze Layer
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
    batch_start_time TIMESTAMP;		
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '==============================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==============================';

    ------------------------------------------------------------------
    -- Load CRM Tables
    ------------------------------------------------------------------
    RAISE NOTICE '------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting data into bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'C:\Anish\SQL_datasets\source_crm\cust_info.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting data into bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'C:\Anish\SQL_datasets\source_crm\prd_info.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting data into bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'C:\Anish\SQL_datasets\source_crm\sales_details.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    ------------------------------------------------------------------
    -- Load ERP Tables
    ------------------------------------------------------------------
    RAISE NOTICE '------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------';

    -- erp_loc_a101
    start_time := clock_timestamp();    
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting data into bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'C:\Anish\SQL_datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- erp_cust_az12
    start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting data into bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'C:\Anish\SQL_datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    -- erp_px_cat_g1v2
    start_time := clock_timestamp(); 
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting data into bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\Anish\SQL_datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true);

    end_time := clock_timestamp();
    RAISE NOTICE 'Load duration: % msec',
        ROUND(EXTRACT(EPOCH FROM end_time - start_time) * 1000, 3);
    RAISE NOTICE '-------------------';

    ------------------------------------------------------------------
    -- Summary
    ------------------------------------------------------------------
    batch_end_time := clock_timestamp();

    RAISE NOTICE '==========================';
    RAISE NOTICE 'Loading Bronze Layer is completed';
    RAISE NOTICE 'Total Load Duration % msec',
        ROUND(EXTRACT(EPOCH FROM batch_end_time - batch_start_time) * 1000, 3);
    RAISE NOTICE '==========================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Error occurred during loading Bronze Layer';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==============================';
END;
$$;

-- Execute procedure
CALL bronze.load_bronze();
