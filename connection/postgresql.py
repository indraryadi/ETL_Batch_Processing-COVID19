import json
from sqlalchemy import create_engine
from psycopg2 import connect
import os
class PostgreSQL:
    def __init__(self,cfg):
        self.host=cfg["host"]
        self.port=cfg["port"]
        self.database=cfg["database"]
        self.username=cfg["username"]
        self.password=cfg["password"]
    
    def conn(self,conn_type='engine'):
        try:
            if conn_type == 'engine':
                engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
                conn_engine = engine.connect()
                print("Connect Engine Postgresql")
                return engine, conn_engine
            else:
                conn = connect(
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database
                    )
                cursor = conn.cursor()
                print("Connect Cursor Postgresql")
                return conn, cursor
        except (Exception) as e:
            print("Cannot Connect!!!")
            return e
        

# if __name__ == "__main__":
    
#     path='/home/hadoop/Documents/ETL_Batch_Processing-COVID19/'
#     with open(path+'credentials.json','r') as d:
#         data=json.load(d)
#     cfg=data['postgresql']
#     postgre=PostgreSQL(cfg)
#     print(postgre.conn())
#     print(cfg)
    