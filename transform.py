# from ntpath import join
# from struct import Struct
import findspark
# from sqlalchemy import null
findspark.init('/home/hadoop/spark-3.0.3-bin-hadoop3.2/')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark=SparkSession.builder.appName('Submitted2').config('spark.jars', 'file:///home/hadoop/postgresql-42.2.6.jar').getOrCreate()
print(spark)

# load from hdfs
rawdf=spark.read.format('csv').option('inferSchema','True').option('header',True).option('separator',',').load('hdfs:///covid19/raw_data_airflow2')

#dim province
df=rawdf.select('kode_prov','nama_prov').distinct()
df=df.withColumnRenamed('kode_prov','province_id').withColumnRenamed('nama_prov','province_name')
# df.show()

#dim disctrict
df1=rawdf.select('kode_kab','kode_prov','nama_kab')
df1=df1.withColumnRenamed('kode_kab','district_id').withColumnRenamed('kode_prov','province_id').withColumnRenamed('nama_kab','district_name')
# print(df1.count())
df1=df1.distinct()
# print(df1.count())
# df1.show(5,truncate=False)

#dim case
df2=rawdf.select(['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

#CREATE LOOP FOR UNPIVOT
# a=['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal']

# col1=[]
# for i in a:
#     col1.append(i)
#     col1.append(i.strip("'"))
# print(col1)


df2=df2.limit(1)
unpivot=df2.selectExpr("stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)")
unpivot=unpivot.distinct().sort('status')

w= Window.orderBy('status')
newdf=unpivot.withColumn("id",row_number().over(w))
newdf=newdf.select(['id','status'])
newdf=newdf.withColumn('status_name',split(newdf['status'],'_').getItem(0)).\
            withColumn('status_detail',split(newdf['status'],'_').getItem(1))
# newdf.show(truncate=False)





# #fact prov daily
# df3 = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

# unpivot2=df3.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])

# unpivot2=unpivot2.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
# # unpivot2.where(unpivot2["tanggal"]=="2020-08-05").show(truncate=False)
# w= Window.orderBy('tanggal')
# newdf2=unpivot2.withColumn("id",row_number().over(w))
# # newdf2.show()
# # print(newdf2.count())

# #join
# newdf=newdf.withColumnRenamed("id","case_id")
# # newdf.show()
# newdf2=newdf2.join(newdf,on="status",how="inner")
# newdf2=newdf2.select(["id","kode_prov","case_id","tanggal","sum(count)"])
# newdf2=newdf2.withColumnRenamed("kode_prov","province_id").\
#               withColumnRenamed("tanggal","date").\
#               withColumnRenamed("sum(count)","total")
# # newdf2.show()
# #RENAME COLUMN



# # fact province monthly
# df4 = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

# unpivot3=df4.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])


# unpivot3=unpivot3.withColumn('month',split(unpivot3['tanggal'],'-').getItem(1))
# unpivot3=unpivot3.withColumn('tanggal',unpivot3['month'])
# unpivot3=unpivot3.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
# unpivot3.drop("month")
# # # unpivot3.where(unpivot3["tanggal"]=="2020-08-05").show(truncate=False)
# w= Window.orderBy('tanggal')
# newdf3=unpivot3.withColumn("id",row_number().over(w))
# # newdf3.show()
# # # print(newdf3.count())
# # unpivot3.show()
# # #join
# newdf=newdf.withColumnRenamed("id","case_id")
# # newdf.show()
# newdf3=newdf3.join(newdf,on="status",how="inner")
# newdf3=newdf3.select(["id","kode_prov","case_id","tanggal","sum(count)"])
# newdf3=newdf3.withColumnRenamed("kode_prov","province_id").\
#               withColumnRenamed("tanggal","month").\
#               withColumnRenamed("sum(count)","total")
# # newdf3.show()



# # fact province yearly
# df5 = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

# unpivot4=df5.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])

# unpivot4=unpivot4.withColumn('yearly',split(unpivot4['tanggal'],'-').getItem(0))
# unpivot4=unpivot4.withColumn('tanggal',unpivot4['yearly'])
# unpivot4=unpivot4.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
# unpivot4.drop("yearly")
# # # unpivot4.where(unpivot4["tanggal"]=="2020-08-05").show(truncate=False)
# w= Window.orderBy('tanggal')
# newdf4=unpivot4.withColumn("id",row_number().over(w))
# # newdf4.show()
# # # print(newdf4.count())
# # unpovot4.show()
# # #join
# newdf=newdf.withColumnRenamed("id","case_id")
# # newdf.show()
# newdf4=newdf4.join(newdf,on="status",how="inner")
# newdf4=newdf4.select(["id","kode_prov","case_id","tanggal","sum(count)"])
# newdf4=newdf4.withColumnRenamed("kode_prov","province_id").\
#               withColumnRenamed("tanggal","yearly").\
#               withColumnRenamed("sum(count)","total")
# # newdf4.show()


#################






#fact district daily
df3 = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

unpivot2=df3.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])

unpivot2=unpivot2.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
# unpivot2.where(unpivot2["tanggal"]=="2020-08-05").show(truncate=False)
w= Window.orderBy('tanggal')
newdf2=unpivot2.withColumn("id",row_number().over(w))
# newdf2.show()
# print(newdf2.count())

#join
newdf=newdf.withColumnRenamed("id","case_id")
# newdf.show()
newdf2=newdf2.join(newdf,on="status",how="inner")
newdf2=newdf2.select(["id","kode_kab","case_id","tanggal","sum(count)"])
newdf2=newdf2.withColumnRenamed("kode_kab","district_id").\
              withColumnRenamed("tanggal","date").\
              withColumnRenamed("sum(count)","total")
newdf2.show()



# fact province monthly
df4 = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

unpivot3=df4.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])


# unpivot3=unpivot3.withColumn('month',split(unpivot3['tanggal'],'-').getItem(0))
unpivot3=unpivot3.withColumn('month',substring('tanggal',1,7))
# unpivot3.show()
unpivot3=unpivot3.withColumn('tanggal',unpivot3['month'])
unpivot3=unpivot3.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
unpivot3.drop("month")
# # # unpivot3.where(unpivot3["tanggal"]=="2020-08-05").show(truncate=False)
w= Window.orderBy('tanggal')
newdf3=unpivot3.withColumn("id",row_number().over(w))
# newdf3.show()
# # print(newdf3.count())
# unpivot3.show()
# #join
newdf=newdf.withColumnRenamed("id","case_id")
# newdf.show()
newdf3=newdf3.join(newdf,on="status",how="inner")
newdf3=newdf3.select(["id","kode_kab","case_id","tanggal","sum(count)"])
newdf3=newdf3.withColumnRenamed("kode_kab","district_id").\
              withColumnRenamed("tanggal","month").\
              withColumnRenamed("sum(count)","total")
newdf3.show()



# fact district yearly
df5 = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

unpivot4=df5.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])

unpivot4=unpivot4.withColumn('yearly',split(unpivot4['tanggal'],'-').getItem(0))
unpivot4=unpivot4.withColumn('tanggal',unpivot4['yearly'])
unpivot4=unpivot4.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
unpivot4.drop("yearly")
# # unpivot4.where(unpivot4["tanggal"]=="2020-08-05").show(truncate=False)
w= Window.orderBy('tanggal')
newdf4=unpivot4.withColumn("id",row_number().over(w))
# newdf4.show()
# # print(newdf4.count())
# unpovot4.show()
# #join
newdf=newdf.withColumnRenamed("id","case_id")
# newdf.show()
newdf4=newdf4.join(newdf,on="status",how="inner")
newdf4=newdf4.select(["id","kode_kab","case_id","tanggal","sum(count)"])
newdf4=newdf4.withColumnRenamed("kode_kab","district_id").\
              withColumnRenamed("tanggal","yearly").\
              withColumnRenamed("sum(count)","total")
newdf4.show()