import findspark
findspark.init('/home/hadoop/spark-3.0.3-bin-hadoop3.2/')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
print(spark)

# load from hdfs
rawdf=spark.read.format("csv").option("header",True).option("separator",",").load("hdfs:///covid19/raw_data_airflow2")

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
df2=rawdf.select("suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal")
col=['id','status_name','status_detail','status']
df2=df2.limit(1)
# df2.show()
df2=df2.toPandas()
df2=df2.melt(var_name='status',value_name='total')
id=[x for x in range(1,len(df2)+1)]
# print(id)
df2['id']=id
df2[['status_name','status_detail']]=df2['status'].str.split('_',n=1,expand=True)
df2=df2[col]
# print(df2)
df2t=spark.createDataFrame(data=df2)

#fact prov daily
column_start = ["tanggal", "kode_prov", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
column_end = ['date', 'province_id', 'status', 'total']

# AGGREGATE
data=rawdf.toPandas()
data = data[column_start]
data = data.melt(id_vars=["tanggal", "kode_prov"], var_name="status", value_name="total").sort_values(["tanggal", "kode_prov", "status"])
# data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
# data = data.reset_index()

print(data)
# # REFORMAT
# data.columns = column_end
# data['id'] = np.arange(1, data.shape[0]+1)
# # MERGE
# dim_case = dim_case.rename({'id': 'case_id'}, axis=1)
# data = pd.merge(data, dim_case, how='inner', on='status')

# data = data[['id', 'province_id', 'case_id', 'date', 'total']]
