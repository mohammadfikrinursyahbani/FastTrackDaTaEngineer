
  
    

  create  table "data_warehouse"."dbt_dev_raw"."film__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."public"."film"
  );
  