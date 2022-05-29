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
    
    load00=load_to_dwh(dim_disc,'dim_district')
    print(load00)
    
    load01=load_to_dwh(dim_cases,'dim_case')
    print(load01)
    
    
    load02=load_to_dwh(dim_prov,'dim_province')
    print(load02)