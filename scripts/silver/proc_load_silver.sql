/*
stored procedure: load silver layer (bronze --> bronze)
Purpose: perform etl to populate silver schema tables from bronze schema
         truncate silver tables, then insert transformed and cleansed data from bronze into silver tables

		 usage: exec bronze.load_silver;
*/
create or alter procedure silver.load_silver as
begin

    -- clean crm customer info
    -- remove duplicates, unwanted spaces, normalize gender and marital values 
    truncate table silver.crm_cust_info;
    print ' inserting data into: silver.crm_cust_info';
    insert into silver.crm_cust_info(	
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
	    trim(cst_firstname) as cst_first_name,
	    trim(cst_lastname) as cst_last_name,
	    case when upper(trim(cst_marital_status)) = 'S' then 'Single'
		    when upper(trim(cst_marital_status)) = 'M' then 'Married'
		    else 'n/a'
	    end cst_marital_status,
	    case when upper(trim(cst_gndr)) = 'F' then 'Female'
		    when upper(trim(cst_gndr)) = 'M' then 'Male'
		    else 'n/a'
	    end cst_gndr,
	    cst_create_date
    from (
    select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last  
    from bronze.crm_cust_info
    where cst_id is not null
    ) t where flag_last = 1;


    -- clean crm product info 
    truncate table silver.crm_prd_info;
    print ' inserting data into: silver.crm_prd_info';
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
        replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
	    substring(prd_key, 7, len(prd_key)) as prd_key,
        prd_nm,
        isnull(prd_cost, 0) as prd_cost,
        CASE  upper(trim(prd_line))
		    WHEN 'M' THEN 'Mountain'
		    WHEN 'R' THEN 'Road'
		    WHEN 'S' THEN 'Other Sales'
		    WHEN 'T' THEN 'Touring'
		    ELSE 'n/a'
	    END AS prd_line,
	    cast(prd_start_dt as date) as prd_start_dt,
        cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt
    FROM bronze.crm_prd_info


    -- clean up crm sales
    truncate table silver.crm_sales_details;
    print ' inserting data into: silver.crm_sales_details';
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
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
	    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
	    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
    
	    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
	    END AS sls_sales,

	    sls_quantity,

	    CASE WHEN sls_price IS NULL OR sls_price <= 0
		     THEN sls_sales / NULLIF(sls_quantity, 0)
		     ELSE sls_price
	    END AS sls_price

    from bronze.crm_sales_details;


    -- clean up erp customer details
    truncate table silver.erp_cust_az12;
    print ' inserting data into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
             ELSE cid
        END cid,
        CASE WHEN bdate > GETDATE() THEN NULL
         ELSE bdate
        END AS bdate,

        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
             ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12


    -- clean up erp customer location 
    truncate table silver.erp_loc_a101;
    print ' inserting data into: silver.erp_loc_a101';
    insert into silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END as cntry
    FROM bronze.erp_loc_a101


    -- clean up erp product categories
    truncate table silver.erp_px_cat_g1v2;
    print ' inserting data into: silver.erp_px_cat_g1v2';
    insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    select id, cat, subcat, maintenance
    from bronze.erp_px_cat_g1v2;

end