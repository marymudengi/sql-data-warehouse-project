/*
======================================================================
Quality Checks
======================================================================
This script checks for quality of the brinze layer tables before loaing into the silver layer.

=============================================================================================

===========bronze.crm_cust_info===============================================================

select * from bronze.crm_cust_info;

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

select cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Checking for unwanted Spaces
-- Expectation is: No Results

Select cst_firstname, TRIM(cst_firstname)
from bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

Select cst_lastname, TRIM(cst_lastname)
from bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

Select cst_marital_status, TRIM(cst_marital_status)
from bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

Select cst_gndr, TRIM(cst_gndr)
from bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

==========bronze.crm_prd_info=========================================================

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')
-----CHECK FOR NULLS OR DUPLICATES

select prd_id,
COUNT(*)
from silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

----Check for unwanted Spaces
---- Expectation: No results

select prd_nm
FROM silver.crm_prd_info
WHERE prd_nm !=TRIM(prd_nm)

---Check For Nulls or Megative Numbers
----Expectation: No results
select prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

---Data Standardization & Consistency
select DISTINCT prd_line
from silver.crm_prd_info;

----Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

select * 
FROM silver.crm_prd_info

----Identify out of range dates
Select DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()


==================


========================bronze.erp_cust_az12=================================

----Identify out of range dates
Select DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()


----Data Stardardization & Consistency

SELECT DISTINCT
gen
FROM bronze.erp_cust_az12;


---Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

----Check for unwanted Spaces
Select * from silver.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-----Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;


----Data Stardardization & Consistency

SELECT DISTINCT
gen
FROM silver.erp_cust_az12;
---Data Standardization & Consistency

=============bronze.erp_loc_a101=====================================

---Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


=============================bronze.erp_px_cat_g1v2=====================
----Check for unwanted Spaces
Select * from bronze.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-----Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2;


