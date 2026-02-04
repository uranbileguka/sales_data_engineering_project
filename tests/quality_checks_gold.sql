--see schema names
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT IN ('pg_catalog','information_schema')
ORDER BY schema_name;

DROP SCHEMA bronze CASCADE;
DROP SCHEMA gold CASCADE;
DROP SCHEMA silver CASCADE;

CREATE SCHEMA silver;
CREATE SCHEMA bronze;
CREATE SCHEMA gold;

-- schema and table names
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog','information_schema')
ORDER BY table_schema, table_name;


-- see 
select * from bronze.crm_sales_details;
select * from erp_loc_info;
select * from marketing_discount_info;


