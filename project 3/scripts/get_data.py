from connector import Connector
from s3 import s3_api

query = f'''
SELECT
    country,
    COUNT(country) AS total,
    current_date AS date
FROM country AS co
INNER JOIN city AS ci
ON co.country_id = ci.country_id
GROUP BY country
'''

conn = Connector()
df = conn.read_query(query, engine="adbc")
df = df.collect()

s3_client = s3_api()
destination = 'datalake/data_result_task_2.parquet'
with s3_client.open(destination, 'wb') as file:
    df.write_parquet(file)

