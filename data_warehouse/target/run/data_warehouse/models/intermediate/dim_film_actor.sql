
  
    

  create  table "data_warehouse"."dbt_dev_intermediate"."dim_film_actor__dbt_tmp"
  
  
    as
  
  (
    
SELECT
    *
FROM "data_warehouse"."dbt_dev_raw"."film_actor"
  );
  