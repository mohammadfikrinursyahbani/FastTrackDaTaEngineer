import connection
import os
import sqlparse
import pandas as pd

if __name__ == '__main__':
  #connection data source
  conf_prod = connection.config('marketplace_prod')
  conn_prod, engine_prod = connection.get_conn(conf_prod, "DataSource")
  cursor_prod = conn_prod.cursor()
  
  #connection dwh
  conf_dwh = connection.config('dwh')
  conn_dwh, engine_dwh = connection.get_conn(conf_dwh, "DWH")
  cursor_dwh = conn_dwh.cursor()

  
  
  #get query string
  path_query_prod = os.getcwd() + "/query/"
  query_prod = sqlparse.format(
    open(path_query_prod + "query.sql","r").read(), strip_comments=True
  ).strip()
  
  
  path_query_dwh = os.getcwd() + "/query/"
  query_dwh = sqlparse.format(
    open(path_query_dwh + "dwh_design.sql","r").read(), strip_comments=True
  ).strip()
  
  
  try:
    #get data
    print("[INFO] Service etl is running...")
    df = pd.read_sql(query_prod, engine_prod)
    
    # print(df)
    
    # create schema dwh
    cursor_dwh.execute(query_dwh)
    conn_dwh.commit()
    # ingest data to dwh
    df.to_sql(
      'dim_orders_kel_3',
      engine_dwh,
      schema='public',
      if_exists='append',
      index=False
    )
    print("[INFO] Service etl is success")
  except Exception as e:
    print("[INFO] Service etl is failed")
    print(str(e))
    
  