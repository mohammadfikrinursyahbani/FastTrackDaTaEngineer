{{
    config(
        materialized="table",
        schema="mart"
    )
}}

SELECT
    CONCAT(da.first_name, ' ', da.last_name) AS actor_name,
    COUNT(dfa.film_id) AS count_roles
FROM {{ ref('dim_actor') }} da
JOIN {{ ref('dim_film_actor') }} dfa ON da.actor_id = dfa.actor_id
GROUP BY actor_name
ORDER BY count_roles DESC