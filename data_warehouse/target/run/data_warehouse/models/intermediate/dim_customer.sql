
  
    

  create  table "data_warehouse"."dbt_dev_intermediate"."dim_customer__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."dbt_dev_raw"."customer"
  );
  