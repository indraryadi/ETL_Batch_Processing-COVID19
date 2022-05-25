from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Submitted2").getOrCreate()
df=spark.read.format("csv").option("header",True).option("separator",",").load("hdfs:///titanic.csv")
# df.write.mode("overwrite").csv("hdfs:///covid19/raw_data_test.csv")
df.write.csv("hdfs:///covid19/raw_data_test.csv")
# df.show(10,False)   