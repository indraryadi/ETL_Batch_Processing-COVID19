from sql.query import create_table_dim, create_table_fact
from connection.postgresql import PostgreSQL
import json

with open ('/home/hadoop/Documents/ETL_Batch_Processing-COVID19/credentials.json', "r") as cred:
    credential = json.load(cred)

def create_star_schema(schema):
  postgre_auth = PostgreSQL(credential['postgresql'])
  conn, cursor = postgre_auth.conn(conn_type='cursor')

  query_dim = create_table_dim(schema=schema)
  cursor.execute(query_dim)
  conn.commit()

  query_fact = create_table_fact(schema=schema)
  cursor.execute(query_fact)
  conn.commit()

  cursor.close()
  conn.close()

if __name__=="__main__":
    create_star_schema('public')