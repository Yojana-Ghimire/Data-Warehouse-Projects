-- 1. Create schema if not exists
CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION postgres;

-- 2. Drop and create CRM tables
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

-- 3. Drop and create crm_sales_details + staging for safe load
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

DROP TABLE IF EXISTS bronze.crm_sales_details_staging;
CREATE TABLE bronze.crm_sales_details_staging (
    sls_ord_num  TEXT,
    sls_prd_key  TEXT,
    sls_cust_id  TEXT,
    sls_order_dt TEXT,
    sls_ship_dt  TEXT,
    sls_due_dt   TEXT,
    sls_sales    TEXT,
    sls_quantity TEXT,
    sls_price    TEXT
);

-- 4. Load CSV into staging table
COPY bronze.crm_sales_details_staging
FROM 'C:/datasets/sales_details.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL ''
);

-- 5. Clean and insert into main table from staging
TRUNCATE TABLE bronze.crm_sales_details;

INSERT INTO bronze.crm_sales_details (
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
    NULLIF(sls_cust_id, '')::INT,
    CASE 
      WHEN sls_order_dt ~ '^\d+$' THEN DATE '1899-12-31' + (sls_order_dt::INT) * INTERVAL '1 day'
      ELSE NULLIF(sls_order_dt, '0')::DATE
    END,
    CASE 
      WHEN sls_ship_dt ~ '^\d+$' THEN DATE '1899-12-31' + (sls_ship_dt::INT) * INTERVAL '1 day'
      ELSE NULLIF(sls_ship_dt, '0')::DATE
    END,
    CASE 
      WHEN sls_due_dt ~ '^\d+$' THEN DATE '1899-12-31' + (sls_due_dt::INT) * INTERVAL '1 day'
      ELSE NULLIF(sls_due_dt, '0')::DATE
    END,
    NULLIF(sls_sales, '')::INT,
    NULLIF(sls_quantity, '')::INT,
    NULLIF(sls_price, '')::INT
FROM bronze.crm_sales_details_staging;

-- 6. Verify rows loaded
SELECT COUNT(*) AS total_loaded FROM bronze.crm_sales_details;

-- 7. Optional: Drop staging table if no longer needed
-- DROP TABLE bronze.crm_sales_details_staging;

-- 8. Drop and create ERP tables
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
