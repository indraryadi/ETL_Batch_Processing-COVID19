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
    
def load_to_dwh():
    pass

if __name__=="__main__":
    spark=sparkSession()