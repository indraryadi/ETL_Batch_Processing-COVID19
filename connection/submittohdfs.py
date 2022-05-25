import findspark
findspark.init('/home/hadoop/spark-3.0.3-bin-hadoop3.2/')
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Submitted2").getOrCreate()
print(spark)
df=spark.read.format("csv").option("header",True).option("separator",",").load("hdfs:///titanic.csv")
# df.write.mode("overwrite").csv("hdfs:///covid19/raw_data_test.csv")
# df.write.csv("hdfs:///covid19/raw_data_test2.csv")
df.show(10,False)   