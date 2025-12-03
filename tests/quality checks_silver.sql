
----Identify out of range dates
Select DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()


----Data Stardardization & Consistency

SELECT DISTINCT
gen
FROM silver.erp_cust_az12;


---Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

----Check for unwanted Spaces
Select * from silver.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-----Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2;
