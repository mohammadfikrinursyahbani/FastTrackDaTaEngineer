import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()

class Connector:
    def __init__(self):
        self.uri_postgres = f"postgresql://{os.getenv('PG_USERNAME')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_SERVER')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
        self.uri_mysql = f"mysql+mysqlconnector://{os.getenv('TI_USERNAME')}:{os.getenv('TI_PASSWORD')}@{os.getenv('TI_URL')}:{os.getenv('TI_PORT')}/{os.getenv('TI_DATABASE')}"

    def read_adbc(self, query):
        df = pl.read_database_uri(query=query, uri=self.uri_postgres, engine="adbc")
        return pl.LazyFrame(df)

    def write_adbc(self, df, table_name):
        df.write_database(connection=self.uri_postgres, table=table_name, engine="adbc")

    def read_connectorx(self, query):
        df = pl.read_database_uri(query=query, uri=self.uri_mysql, engine="connectorx")
        return pl.LazyFrame(df)

    def write_connectorx(self, df, table_name):
        df.write_database(table_name=table_name,  connection=self.uri_mysql)

    def read_query(self, query, engine):
        '''
        query: str
            SQL query
        engine: str
            "adbc" or "connectorx"
        '''
        if engine == "adbc":
            return self.read_adbc(query)
        elif engine == "connectorx":
            return self.read_connectorx(query)
        else:
            raise ValueError("Engine not supported")

if __name__ == "__main__":
    conn = Connector()
    query = "SELECT * FROM country"
    df = conn.read_query(query, engine="adbc")
    print(df)