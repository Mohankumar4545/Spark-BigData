from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("first-hdfs").getOrCreate()


df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///home/mohankumar-r/Spark/SparkLearning/customers-100000.csv")

df1=df.limit(10)

df1.write.mode("overwrite").save("file:///home/mohankumar-r/Spark/SparkLearning/customers-output.csv")
