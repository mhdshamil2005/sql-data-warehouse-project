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
    EXEC Silver.load_silver;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME2,@end_time DATETIME2,@batch_start_time DATETIME2,@batch_end_time DATETIME2;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '============================================';
		PRINT 'Loading Silver Layer';
		PRINT '============================================';

		PRINT '--------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE Silver.crm_cust_info;

		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(trim(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(trim(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN cst_marital_status is NULL then 'n/a'
		END as cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN cst_gndr is null then 'n/a'
		END as cst_gndr,
		cst_create_date
		from (
		select
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as Dupli
		from Bronze.crm_cust_info
		where cst_id is not null
		)t
		where Dupli =1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE Silver.crm_prd_info;

		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO Silver.crm_prd_info (
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
		REPLACE (SUBSTRING (prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING (prd_key,7,LEN (prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost,0) prd_cost,
		CASE upper(trim(prd_line))
			WHEN 'M' then 'Mountain'
			WHEN 'R' then 'Road'
			WHEN 'S' then 'Other Sales'
			WHEN 'T' then 'Touring'
			ELSE 'n/a'
		END as prd_line,
		prd_start_dt,
		DATEADD (day,-1,LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt
		FROM Bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+ ' seconds';
		PRINT '--------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO Silver.crm_sales_details (
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
		CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
		end as sls_order_dt,
		CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
		end as sls_ship_dt,
		CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
		end as sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity* ABS(sls_price)
				THEN sls_quantity* ABS(sls_price)
			 ELSE sls_sales
		END as sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price
		END as sls_price
		FROM Bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------------';


		PRINT '--------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE Silver.erp_cust_az12;

		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO Silver.erp_cust_az12 (
			cid,bdate,gen)

		select
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,len(cid))
			 ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END bdate,
		CASE WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN upper(trim(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
		END gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '--------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		select
		CASE WHEN cid like 'AW-%' THEN REPLACE(cid,'-','')
			 ELSE cid
		END cid,
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry) 
		END cntry
		from Bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '--------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		select
		id,
		cat,
		subcat,
		maintenance
		from Bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------------';
		

		PRINT '============================================';
		PRINT 'Loading Silver Layer is Completed';
		SET @batch_end_time = GETDATE();

		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================';

	END TRY
	BEGIN CATCH
		PRINT '============================================';
		PRINT 'ERROR OCCURED DURING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR) ;
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================';
	END CATCH
END
