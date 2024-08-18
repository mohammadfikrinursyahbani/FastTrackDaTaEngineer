
  
    

  create  table "data_warehouse"."dbt_dev_raw"."rental__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."public"."rental"
  );
  