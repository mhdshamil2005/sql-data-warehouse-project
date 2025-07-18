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
-- Checking Bronze Layer
-- ====================================================================

--- CHECK Bronze.crm_cust_info ---	  
select
cst_id,
COUNT(*)
from Bronze.crm_cust_info
group by cst_id
having COUNT(*) > 1 or cst_id is NULL

select cst_key
from Bronze.crm_cust_info
where trim (cst_key) != cst_key

select cst_firstname
from Bronze.crm_cust_info
where TRIM (cst_firstname) != cst_firstname

select cst_lastname
from Bronze.crm_cust_info
where TRIM (cst_lastname) != cst_lastname

select distinct cst_marital_status
from Bronze.crm_cust_info

select distinct cst_gndr
from Bronze.crm_cust_info


--- CHECK Bronze.crm_prd_info ---	
select
prd_id,
count(*)
from Bronze.crm_prd_info
group by prd_id
having count(*) > 1 

select prd_nm
from Bronze.crm_prd_info
where trim(prd_nm) != prd_nm

select prd_cost
from Bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

select distinct prd_line
from Silver.crm_prd_info

select*
from Bronze.crm_prd_info
where prd_end_dt < prd_start_dt

select*,
LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) as prd_end_dt
from Bronze.crm_prd_info


--- CHECK Bronze.crm_sales_details ---
select *,sls_ord_num
from bronze.crm_sales_details
where trim(sls_ord_num) != sls_ord_num

SELECT sls_prd_key
FROM Bronze.crm_sales_details
where sls_prd_key NOT IN (select distinct prd_key from Silver.crm_prd_info)

SELECT sls_cust_id
FROM Bronze.crm_sales_details
where sls_cust_id not IN (select distinct cst_id from Silver.crm_cust_info)

select
nullif(sls_order_dt,0)
from Bronze.crm_sales_details
where sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

select
sls_ship_dt
from Bronze.crm_sales_details
where sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101

select
sls_due_dt
from Bronze.crm_sales_details
where sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

select* from Bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

select 
sls_sales,
sls_quantity,
sls_price,
CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END,
sls_quantity,
CASE WHEN sls_price is null or sls_price <= 0
		THEN ABS(sls_sales)/nullif(sls_quantity,0)
	ELSE sls_price
END
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales


--- CHECK Bronze.erp_cust_az12 ---

select
cid,
CASE WHEN cid LIKE 'NAS%' THEN REPLACE (cid,'NAS','')
	 ELSE cid
END
from bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN REPLACE (cid,'NAS','')
	 ELSE cid end
	 not in (
select distinct cst_key from Bronze.crm_cust_info)

select bdate
from Bronze.erp_cust_az12
where bdate > GETDATE()

select gen
from Bronze.erp_cust_az12


--- CHECK Bronze.erp_loc_a101 ---
select distinct cid
from Bronze.erp_loc_a101

select
CASE WHEN cid like 'AW-%' THEN REPLACE(cid,'-','')
	 ELSE cid
END cid
from Bronze.erp_loc_a101
where CASE WHEN cid like 'AW-%' THEN REPLACE(cid,'-','')
	 ELSE cid end
	  not in (
select cst_key from bronze.crm_cust_info)

SELECT DISTINCT
cntry,
CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry) 
END cntry
from Bronze.erp_loc_a101

select distinct cntry
from silver.erp_loc_a101


--- CHECK Bronze.erp_px_cat_g1v2 ---
select id
from Bronze.erp_px_cat_g1v2
where id not in (
select cat_id from Silver.crm_prd_info)

select distinct
cat
from Bronze.erp_px_cat_g1v2

select distinct
subcat
from Bronze.erp_px_cat_g1v2

select distinct
maintenance
from Bronze.erp_px_cat_g1v2

select
maintenance
from Bronze.erp_px_cat_g1v2
where maintenance != trim(maintenance)


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
