import findspark
findspark.init('/home/hadoop/spark-3.0.3-bin-hadoop3.2/')
from pyspark.sql import SparkSession
import os

spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
print(spark)

#load from hdfs
df=spark.read.format("csv").option("header",True).option("separator",",").load("hdfs:///titanic.csv")

# load from local
# df=spark.read.format("csv").option("header",True).option("separator",",").load("file:///home/hadoop/Documents/ETL_Batch_Processing-COVID19/dataset/data_covid.json")

#save to hdfs
# df.write.mode("overwrite").csv("hdfs:///covid19/raw_data_test.csv")
# df.write.csv("hdfs:///covid19/raw_data_test2.csv")

#save to postgre
mode = "overwrite"
url = "jdbc:postgresql://localhost:5432/covid19"
properties = {"user": "postgres","password": "indra24","driver": "org.postgresql.Driver"}

df.write.jdbc(url=url, table="test_result", mode=mode, properties=properties)

# df.write \
#     .jdbc("jdbc:postgresql://localhost:5432/covid19", "public.tablename",
#           properties={"user": "postgres", "password": "indra24"})


# df.show(10,False)   