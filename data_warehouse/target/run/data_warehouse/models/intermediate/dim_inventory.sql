
  
    

  create  table "data_warehouse"."dbt_dev_intermediate"."dim_inventory__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."dbt_dev_raw"."inventory"
  );
  