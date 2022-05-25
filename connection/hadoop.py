from importlib.resources import contents
from hdfs import InsecureClient
import json
import pandas as pd
class Hadoop:
    def __init__(self,cfg):
        self.ip=cfg["ip"]
        self.user=cfg["user"]
    def client(self):
        try:
            client = InsecureClient(self.ip)
            return client
        except (Exception) as e:
            print("ERRORR!!!")
            return e
    
if __name__=="__main__":
    with open('credentials.json','r') as d:
        data=json.load(d)
    cfg=data['hadoop']
    hadoop=Hadoop(cfg)
    client=hadoop.client()
    print(client)
    data=pd.read_json('data/data_covid.json','r')
    df=pd.DataFrame(data['data']['content'])
    
    # with client.write(f'/covid19/test.csv',encoding='utf-8') as writer:
    #     df.to_csv(writer,index=False)
