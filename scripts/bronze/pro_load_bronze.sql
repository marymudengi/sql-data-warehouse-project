SELECT name 
FROM sys.schemas
WHERE name = 'bronze';

USE DataWarehouse;

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
            SET @batch_start_time = GETDATE();
            PRINT '=============================================';
            PRINT 'Loading Bronze Layer'
            PRINT '=============================================';

            PRINT'_______________________________________________';
            PRINT'Loading CRM Tables';
            PRINT'_______________________________________________';


            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_cust_info;

            BULK INSERT bronze.crm_cust_info
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
            WITH (FIRSTROW =2,
            FIELDTERMINATOR =',',
            TABLOCK
            );
                 SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';
            
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_prd_info;

            BULK INSERT bronze.crm_prd_info
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
            WITH (FIRSTROW =2,
            FIELDTERMINATOR =',',
            TABLOCK
            );
             SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';

              SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_sales_details;

            BULK INSERT bronze.crm_sales_details
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
            WITH (FIRSTROW =2,
            FIELDTERMINATOR =',',
            TABLOCK
            );
             SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';

            PRINT'_______________________________________________';
            PRINT'Loading ERP Tables';
            PRINT'_______________________________________________';

              SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_cust_az12;

            BULK INSERT bronze.erp_cust_az12
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_az12.csv'
            WITH ( FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK);
             SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';

            
              SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_loc_a101;

            BULK INSERT bronze.erp_loc_a101
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
            WITH (FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
             SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';


              SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_px_cat_g1v2;

            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'C:\Users\msinvula\OneDrive - Road Fund Administration\Desktop\SQL Datawarehouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
            WITH (FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
             SET @end_time = GETDATE();
                 PRINT'>> Load Duration:'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                 PRINT '_________________________________';
            
 SET @batch_end_time= GETDATE();
 PRINT'================================='
 PRINT'Loading Bronze Layer is Completed';
 PRINT'  ---Total Load Duration:' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
 PRINT '================================'


END TRY
BEGIN CATCH
PRINT '========================================================'
PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
PRINT'Error Message' + ERROR_MESSAGE();
PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '========================================================'
END CATCH
END

