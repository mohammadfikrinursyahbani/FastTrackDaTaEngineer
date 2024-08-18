
  
    

  create  table "data_warehouse"."dbt_dev_intermediate"."dim_staff__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."dbt_dev_raw"."staff"
  );
  