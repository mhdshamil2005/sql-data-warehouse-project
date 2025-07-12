/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	CASE WHEN ci.cst_gndr = 'n/a' then COALESCE(ca.gen,'n/a')
		 WHEN ci.cst_gndr != 'n/a' then COALESCE(ci.cst_gndr,'n/a')
	END as gender,
	ci.cst_marital_status as marital_status,
	ci.cst_create_date as create_date,
	ca.bdate as birthdate
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON ca.cid = ci.cst_key
LEFT JOIN Silver.erp_loc_a101 la
ON la.cid = ci.cst_key
GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products as
select
	ROW_NUMBER() over (order by prd_key) product_key,
	pf.prd_id as product_id,
	pf.prd_key as product_number,
	pf.prd_nm as product_name,
	pf.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance as maintenance,
	pf.prd_cost as cost,
	pf.prd_line as product_line,
	pf.prd_start_dt as start_date
	from Silver.crm_prd_info pf
	LEFT JOIN Silver.erp_px_cat_g1v2 pc
	on pf.cat_id = pc.id
where prd_end_dt IS NULL
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales as
select  
	s.sls_ord_num as order_number,
	c.customer_key,
	p.product_key,
	s.sls_order_dt as order_date,
	s.sls_ship_dt as shipping_date,
	s.sls_due_dt as due_date,
	s.sls_sales as sales_amount,
	s.sls_quantity as quantity,
	s.sls_price as price
from Silver.crm_sales_details s
left join gold.dim_customers c
on s.sls_cust_id = c.customer_id
left join gold.dim_products p
on s.sls_prd_key = p.product_number
GO
