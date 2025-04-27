from pyspark.sql import SparkSession

def SessionCreator(Appname):
  return SparkSession.builder\
    .appName(Appname) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", "AccessKey") \
    .config("spark.hadoop.fs.s3a.secret.key", "Secret key") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
    .getOrCreate()
