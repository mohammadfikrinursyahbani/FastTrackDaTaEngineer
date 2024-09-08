from connector import Connector
from s3 import s3_api
import polars as pl

client = s3_api()
file_path = 'datalake/data_result_task_2.parquet'
with client.open(file_path, 'rb') as file:
    df = pl.read_parquet(file)

conn = Connector()
conn.write_connectorx(df, table_name='top_country')