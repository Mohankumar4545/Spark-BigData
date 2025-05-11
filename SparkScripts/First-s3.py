from pyspark.sql import SparkSession
from utils.SparkSession import SessionCreator
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
load_dotenv()
spark=SessionCreator("FirstS3")

#Database connection 
jdbcURL = f"jdbc:postgresql://{os.getenv('PostgresHost')}:{os.getenv('PostgresPort')}/postgres"
DBproperties={
    "user":os.getenv('PostgresUsername'),
    "password":os.getenv('PostgresPassword'),
    "driver":"org.postgresql.Driver"
}

df=spark.read.format("csv")\
    .option("inferSchema","true").option("header","true").\
    load("s3a://spark-test-aprl/Spark-Input-Files/Customer-Data/customers-100000.csv")
df1=df.limit(1000)

df1.write.jdbc(url=jdbcURL,table="Customers",mode="overwrite",properties=DBproperties)