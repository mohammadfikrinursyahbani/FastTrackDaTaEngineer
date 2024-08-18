
  
    

  create  table "data_warehouse"."dbt_dev_raw"."actor__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."public"."actor"
  );
  