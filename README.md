# ETL_Batch_Processing-COVID19

![image](https://user-images.githubusercontent.com/103250258/170876139-0d42153c-0470-4162-8ca1-db30035d2a69.png)
>This project is demonstrate ETL Batch Processing using `HDFS as a data lake`, `pyspark as a data processing` and `airflow as an orchestrator`. The purpose of this project is to create data pipeline from raw data to data lake (HDFS), transform and processing it into dim table and fact table using pyspark and store it into data warehouse (postgreSQL). All the process will be scheduled to do everyday using `Airflow` as scheduler and monitoring process. For the dataset i use 'covid-19 data from Jawa Barat' in json format.
___
### Table of Contents

- Prepare the Dataset
- Design ERD
- Extract Dataset to MySQL
- Transform Raw Data
- Load to PostgreSQL
- Create DAG
- Result

# 1. Prepare the Dataset
![image](https://user-images.githubusercontent.com/103250258/170876643-a0111318-e589-4f86-8a33-08865562a738.png)
>This raw data i got from digital skola tutor
___
# 2. Design ERD
![image](https://user-images.githubusercontent.com/103250258/170877267-29459c40-67b6-48e0-8432-669034dfc16d.png)

# 3. Extract Dataset to HDFS
I create a file called [`ingest.py`](ingest.py) that has job to load the dataset from local into HDFS using `write` method from `pyspark`.
```python
...
data.write.mode("overwrite").option("header",True).csv("hdfs:///covid19/raw_data_airflow2")
...
```
# 4. Transform Raw Data
I transform the raw covid19 Jawa Barat dataset into 3 dimension tables [`[dim_province,dim_district,dim_case]`](trans_load_dim.py) and 6 fact tables [`[fact_province_daily, fact_province_monthly, fact_province_yearly, fact_district_daily, fact_district_monthly, fact_district_yearly]`](trans_load_fact.py).
# 5. Load to PostgreSQL
To load data i use [`postgeSQL JDBC Driver`](https://jdbc.postgresql.org/) that allows our programs to connect to a PostgreSQL database using standard, database independent Java code. The way i used it is decalre it at the config when i `create spark session`.
```python
spark=SparkSession.builder.appName("Submitted2").config("spark.jars", "file:///home/hadoop/postgresql-42.2.6.jar").getOrCreate()
```
# 6. Create DAG
Last for doing this task every day automaticaly, i use airflow as an orchestrator. To do that i need to create [`DAG`](dags/testingDAG.py) (Directed Acyclic Graph). in this file i scheduled the task to repeat daily using cron_preset on airflow that present as `@daily` and using airflow `BashOperator` for running the python using bash command. the DAG graph will be look like this:
![image](https://user-images.githubusercontent.com/103250258/170879243-a1302d87-bbd5-49a6-abd1-3a2e7454c61d.png)

# 7. Result
After the DAG running successfully the data lake (HDFS) and the data warehouse (PostgreSQL) will be look like this:
![image](https://user-images.githubusercontent.com/103250258/170879485-2ef4a147-7de1-430e-85a3-e36993aada27.png)
![image](https://user-images.githubusercontent.com/103250258/170879576-ba5e702f-70ba-43af-9e87-42abbd110832.png)
![image](https://user-images.githubusercontent.com/103250258/170879660-b500ec4a-2c51-4c9c-ac11-62d24a9c31e2.png)

