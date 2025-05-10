from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()  

def SessionCreator(Appname):
  awsAccessKey=os.getenv("awsAccessKey")
  awsSecretkey=os.getenv("SecretAccessKey")
  s3EndPoint=os.getenv("S3_ENDPOINT")

  return SparkSession.builder\
    .appName(Appname) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", awsAccessKey) \
    .config("spark.hadoop.fs.s3a.secret.key", awsSecretkey) \
    .config("spark.hadoop.fs.s3a.endpoint", s3EndPoint) \
    .getOrCreate()
