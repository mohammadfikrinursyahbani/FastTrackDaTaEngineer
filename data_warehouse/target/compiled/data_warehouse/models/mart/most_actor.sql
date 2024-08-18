

SELECT
    CONCAT(da.first_name, ' ', da.last_name) AS actor_name,
    COUNT(dfa.film_id) AS count_roles
FROM "data_warehouse"."dbt_dev_intermediate"."dim_actor" da
JOIN "data_warehouse"."dbt_dev_intermediate"."dim_film_actor" dfa ON da.actor_id = dfa.actor_id
GROUP BY actor_name
ORDER BY count_roles DESC