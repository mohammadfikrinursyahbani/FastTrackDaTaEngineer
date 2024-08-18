
  
    

  create  table "data_warehouse"."dbt_dev_intermediate"."dim_rental__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."dbt_dev_raw"."rental"
  );
  