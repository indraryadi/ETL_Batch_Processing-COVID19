# from struct import Struct
# import findspark
# from sqlalchemy import null
# findspark.init('/home/hadoop/spark-3.0.3-bin-hadoop3.2/')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

def sparkSession():
    try:
        print("TRY CREATE SESSION...")
        spark=SparkSession.builder.appName('Submitted2').\
                               config('spark.jars', 'file:///home/hadoop/postgresql-42.2.6.jar').\
                               getOrCreate()
        print("SESSION CREATED!!!")
        return spark
    except (Exception) as e:
        print("SESSION NOT CREATED!!!")
        return e

# load from hdfs
def loadData(sparkSession):
    try:
        print("LOAD DATA...")
        rawdf=sparkSession.read.format('csv').\
                       option('inferSchema','True').\
                       option('header',True).\
                       option('separator',',').\
                       load('hdfs:///covid19/raw_data_airflow2')
        print("DATA LOADED!!!")
        return rawdf
    except (Exception) as e:
        print("DATA NOT LOADED!!!")
        return e
    
#dim province
def dim_province(rawDf):
    try:
        print("TRY CREATE DIM PROVINCE...")
        df=rawDf.select('kode_prov','nama_prov').distinct()
        df=df.withColumnRenamed('kode_prov','province_id').\
             withColumnRenamed('nama_prov','province_name')
        print("DIM PROVINCE CREATED!!!")
        return df
    except (Exception) as e:
        print("DIM PROVINCE NOT CREATED!!!")
        return e

#dim disctrict
def dim_distric(rawDf):
    try:
        print("TRY CREATE DIM DISCTRICT...")
        df=rawDf.select('kode_kab','kode_prov','nama_kab')
        df=df.withColumnRenamed('kode_kab','district_id').\
          withColumnRenamed('kode_prov','province_id').\
          withColumnRenamed('nama_kab','district_name')
        df=df.distinct()
        print("DIM DISTRICT CREATED!!!")
        return df
    except (Exception) as e:
        print("DIM DISTRICT NOT CREATED!!!")
        return e
    
#dim case
def dim_case(rawDf):
    try:
        print("TRY CREATED DIM CASE...")
        df=rawDf.select(['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])

    #CREATE LOOP FOR UNPIVOT
    # a=['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal']
    # col1=[]
    # for i in a:
    #     col1.append(i)
    #     col1.append(i.strip("'"))
    # print(col1)


        df=df.limit(1)
        df=df.selectExpr("stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)")
    
        df=df.distinct().sort('status')

        #add id column
        w= Window.orderBy('status')
        newdf=df.withColumn("id",row_number().over(w))
        newdf=newdf.select(['id','status'])

        #split the status into 2 different columns
        newdf=newdf.withColumn('status_name',split(newdf['status'],'_').getItem(0)).\
                   withColumn('status_detail',split(newdf['status'],'_').getItem(1))
        print("DIM CASE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("DIM CASE NOT CREATED!!!")
        return e
    
#fact prov daily
def fact_prov_daily(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_prov","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_prov","province_id").\
                      withColumnRenamed("tanggal","date").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e
    
#fact prov monthly
def fact_prov_monthly(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])
        # unpivot=unpivot.withColumn('month',split(unpivot['tanggal'],'-').getItem(1))
        unpivot=unpivot.withColumn('month',substring('tanggal',1,7))
        unpivot=unpivot.withColumn('tanggal',unpivot['month'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
        unpivot.drop("month")
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_prov","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_prov","province_id").\
                      withColumnRenamed("tanggal","month").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e

#fact prov yearly
def fact_prov_yearly(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_prov","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_prov','status'])
        unpivot=unpivot.withColumn('year',split(unpivot['tanggal'],'-').getItem(0))
        unpivot=unpivot.withColumn('tanggal',unpivot['year'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_prov', 'status']).sum('count')
        unpivot.drop("year")
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_prov","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_prov","province_id").\
                      withColumnRenamed("tanggal","year").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e
    
 #fact district daily
def fact_district_daily(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_kab","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_kab","district_id").\
                      withColumnRenamed("tanggal","date").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e   

#fact district monthly
def fact_district_monthly(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])
        # unpivot=unpivot.withColumn('month',split(unpivot['tanggal'],'-').getItem(1))
        unpivot=unpivot.withColumn('month',substring('tanggal',1,7))
        unpivot=unpivot.withColumn('tanggal',unpivot['month'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
        unpivot.drop("month")
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_kab","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_kab","district_id").\
                      withColumnRenamed("tanggal","month").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e

#fact district yearly
def fact_district_yearly(rawdf,dimcase):
    
    try:
        print("TRY CREATE FACT TABLE...")
        df = rawdf.select(['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'suspect_meninggal', 'closecontact_meninggal', 'probable_meninggal'])
        unpivot=df.selectExpr("tanggal","kode_kab","stack(11,'suspect_diisolasi', suspect_diisolasi, 'suspect_discarded', suspect_discarded, 'closecontact_dikarantina', closecontact_dikarantina, 'closecontact_discarded', closecontact_discarded, 'probable_diisolasi', probable_diisolasi, 'probable_discarded', probable_discarded, 'confirmation_sembuh', confirmation_sembuh, 'confirmation_meninggal', confirmation_meninggal, 'suspect_meninggal', suspect_meninggal, 'closecontact_meninggal', closecontact_meninggal, 'probable_meninggal', probable_meninggal) as (status,count)").sort(['tanggal','kode_kab','status'])
        unpivot=unpivot.withColumn('year',split(unpivot['tanggal'],'-').getItem(0))
        unpivot=unpivot.withColumn('tanggal',unpivot['year'])
        unpivot=unpivot.groupBy(['tanggal', 'kode_kab', 'status']).sum('count')
        unpivot.drop("year")
        # unpivot.where(unpivot["tanggal"]=="2020-08-05").show(truncate=False)
        w= Window.orderBy('tanggal')
        newdf=unpivot.withColumn("id",row_number().over(w))
        # newdf2.show()
        # print(newdf2.count())
        #join
        dimcase=dimcase.withColumnRenamed("id","case_id")
        # dimcase.show()
        newdf=newdf.join(dimcase,on="status",how="inner")
        newdf=newdf.select(["id","kode_kab","case_id","tanggal","sum(count)"])
        newdf=newdf.withColumnRenamed("kode_kab","district_id").\
                      withColumnRenamed("tanggal","year").\
                      withColumnRenamed("sum(count)","total")
        print("FACT TABLE CREATED!!!")
        return newdf
    except (Exception) as e:
        print("FACT TABLE NOT CREATED!!!")
        return e

#LOAD TO POSTGRESQL
def load_to_dwh(df,tb_name):
    # import json
    try:
        print(f"TRY LOAD {tb_name} TO DWH...")
        with open('/home/hadoop/Documents/ETL_Batch_Processing-COVID19/credentials.json','r') as d:
            data=json.load(d)

        db=data['postgresql']['database']
        user=data['postgresql']['username']
        password=data['postgresql']['password']

        mode = "overwrite"
        url = f"jdbc:postgresql://localhost:5432/{db}"
        properties = {"user": user,"password": password,"driver": "org.postgresql.Driver"}

        df.write.jdbc(url=url, table=tb_name, mode=mode, properties=properties)
        # mode = "overwrite"
        # url = "jdbc:postgresql://localhost:5432/covid19"
        # properties = {"user": "postgres","password": "indra24","driver": "org.postgresql.Driver"}
        # df.write.jdbc(url=url, table='dim_province1', mode=mode, properties=properties)
        
        print("LOAD TO DWH SUCESS!!!")
        return "LOAD TO DWH SUCESS!!!"
    except (Exception) as e:
        print("LOAD TO DWH FAILED!!!")
        return e
    
if __name__=="__main__":
    spark=sparkSession()
    data=loadData(spark)
    dim_prov=dim_province(data)
    dim_disc=dim_distric(data)
    dim_cases=dim_case(data)
    fact_province_daily=fact_prov_daily(data,dim_cases)
    fact_province_month=fact_prov_monthly(data,dim_cases)
    fact_province_year=fact_prov_yearly(data,dim_cases)
    fact_disc_daily=fact_district_daily(data,dim_cases)
    fact_disc_month=fact_district_monthly(data,dim_cases)
    fact_disc_year=fact_district_yearly(data,dim_cases)
    
    load00=load_to_dwh(dim_disc,'dim_district')
    print(load00)
    
    load01=load_to_dwh(dim_cases,'dim_case')
    print(load01)
    
    
    load02=load_to_dwh(dim_prov,'dim_province')
    print(load02)
    
    # load dim case from postgre or function???
    
    fact_province_daily.show()
    load1=load_to_dwh(fact_province_daily,'fact_province_daily')
    print(load1)
    
    fact_province_month.show()
    load2=load_to_dwh(fact_province_month,'fact_province_monthly')
    print(load2)
    
    fact_province_year.show()
    load3=load_to_dwh(fact_province_year,'fact_province_yearly')
    print(load3)
    
    fact_disc_daily.show()
    load11=load_to_dwh(fact_disc_daily,'fact_district_daily')
    print(load11)
    
    fact_disc_month.show()
    load22=load_to_dwh(fact_disc_month,'fact_district_monthly')
    
    print(load22)
    
    fact_disc_year.show()
    load33=load_to_dwh(fact_disc_year,'fact_district_yearly')
    print(load33)
    
    # ./spark-3.0.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev --driver-class-path /home/hadoop/postgresql-42.2.6.jar --jars /home/hadoop/postgresql-42.2.6.jar ~/Documents/ETL_Batch_Processing-COVID19/transforms.py
# 