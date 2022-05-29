from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
def sparkSession():
    try:
        spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
        # print(spark)
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
    
def load_dim_case(sparkSession,tb_name):
    #load from postgre
    try:
        print(f"TRY LOAD {tb_name} TO TRANSFORM...")
        with open('/home/hadoop/Documents/ETL_Batch_Processing-COVID19/credentials.json','r') as d:
            data=json.load(d)

        db=data['postgresql']['database']
        user=data['postgresql']['username']
        password=data['postgresql']['password']


        url = f"jdbc:postgresql://localhost:5432/{db}"
        jdbcDF = sparkSession.read \
            .format("jdbc") \
            .option("url", url)\
            .option("dbtable", tb_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return jdbcDF
    except (Exception) as e:
        print(f"{tb_name} NOT LOADED!!!")
        return e

if __name__=="__main__":
    spark=sparkSession()
    data=loadData(spark)
    dim_cases=load_dim_case(spark,"dim_case")
    # dim_case.show()
    # print(type(dim_case))
    
    
    fact_province_daily=fact_prov_daily(data,dim_cases)
    fact_province_month=fact_prov_monthly(data,dim_cases)
    fact_province_year=fact_prov_yearly(data,dim_cases)
    fact_disc_daily=fact_district_daily(data,dim_cases)
    fact_disc_month=fact_district_monthly(data,dim_cases)
    fact_disc_year=fact_district_yearly(data,dim_cases)
    
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