

SELECT 
    df.title, 
    COUNT(fp.payment_id) AS total_sales
FROM "data_warehouse"."dbt_dev_intermediate"."dim_film" df
JOIN "data_warehouse"."dbt_dev_intermediate"."dim_inventory" di ON df.film_id = di.film_id
JOIN "data_warehouse"."dbt_dev_intermediate"."dim_rental" dr ON dr.inventory_id = di.inventory_id
JOIN "data_warehouse"."dbt_dev_intermediate"."fact_payment" fp ON fp.rental_id = dr.rental_id
GROUP BY df.title
ORDER BY total_sales DESC