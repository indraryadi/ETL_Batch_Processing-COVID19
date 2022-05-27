from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def sparkSession():
    try:
        spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
        # print(spark)
        print("SESSION CREATED!!!")
        return spark
    except (Exception) as e:
        print("SESSION NOT CREATED!!!")
        return e
# load from local
def ingest_data(sparkSession):
    try:
        print("READ DATA FROM SOURCE...")
        df=sparkSession.read.option("multiLine",True).option("inferSchema",True).json("file:///home/hadoop/Documents/ETL_Batch_Processing-COVID19/dataset/data_covid.json")
        content=df.select("data.content").select(explode("content"))

        temp_col=[]
        for i in content.schema['col'].dataType:
            temp_col.append("col."+i.name)
        temp_col=[x.lower() for x in temp_col]
        df2=content.select(temp_col)
        print("READ DATA SUCCESS!!!")
        return df2
    except (Exception) as e:
        print("READ DATA FAILED!!!")
        return e
    #save to hdfs
def save_to_hdfs(data):
    # df.write.mode("overwrite").csv("hdfs:///covid19/raw_data_test.csv")
    try:
        print("LOAD TO HDFS...")
        data.write.mode("overwrite").option("header",True).csv("hdfs:///covid19/raw_data_airflow2")
        print("LOAD DATA SUCCESS!!!")
    except (Exception) as e:
        print("LOAD DATA FAILED!!!")
        return e

#bash command to run on spark 
# ./spark-submit --master yarn --queue dev ~/Documents/ETL_Batch_Processing-COVID19/connection/submittohdfs.py 
if __name__=="__main__":
    spark=sparkSession()
    data=ingest_data(spark)
    save_to_hdfs(data)