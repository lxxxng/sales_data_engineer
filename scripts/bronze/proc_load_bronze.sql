/*
stored procedure: load bronze layer (source --> bronze)
Purpose: loads data into bronze schema from external csv files 
		 truncates bronze table before loading data
		 uses bulk insert 

		 usage: exec bronze.load_bronze;
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, 
			@end_time datetime, 
			@batch_start_time datetime, 
			@batch_end_time datetime;
	begin try 
		print('=======================================================')
		print 'loading bronze layer';
		print('=======================================================')

		set @start_time = getdate();
		set @batch_start_time = getdate();

		print('-------------------------------------------------------')
		print('loading crm cust info')
		-- insert crm cust info
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate();
		print('-------------------------------------------------------')
		print('loading crm product info')
		-- insert crm product info
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate();
		print('-------------------------------------------------------')
		print('loading crm sales info')
		-- insert crm sales details 
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate();
		print('-------------------------------------------------------')
		print('loading erp loc details')
		-- insert erp loc details
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate();
		print('-------------------------------------------------------')
		print('loading erp cust details')
		-- insert erp cust details
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'

		set @start_time = getdate();
		print('-------------------------------------------------------')
		print('loading erp cat details')
		-- insert erp cat details
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\wu_li\OneDrive\Desktop\Sales Data Engineer\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print('-------------------------------------------------------')

		set @batch_end_time = getdate()
		print '>> Loading of bronze layer is completed, ' +
					'total load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds' 

	end try
	begin catch
		print '========================================================' 
		print 'Error Occured' 
		print 'Error message' + error_message();
		print 'Error message' + cast(error_number() as nvarchar);
		print 'Error message' + cast(error_state() as nvarchar);
		print '========================================================' 

	end catch
end