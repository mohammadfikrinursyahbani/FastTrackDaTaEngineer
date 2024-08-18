{{
    config(
        materialized="table",
        schema="mart"
    )
}}

SELECT 
    df.title, 
    COUNT(fp.payment_id) AS total_sales
FROM {{ ref('dim_film') }} df
JOIN {{ ref('dim_inventory') }} di ON df.film_id = di.film_id
JOIN {{ ref('dim_rental') }} dr ON dr.inventory_id = di.inventory_id
JOIN {{ ref('fact_payment') }} fp ON fp.rental_id = dr.rental_id
GROUP BY df.title
ORDER BY total_sales DESC
