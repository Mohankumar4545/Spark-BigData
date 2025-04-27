from pyspark.sql import SparkSession
from utils.SparkSession import SessionCreator
from pyspark.sql.functions import *

spark=SessionCreator("FirstS3")

df=spark.read.format("csv")\
    .option("inferSchema","true").option("header","true").\
    load("s3a://spark-test-aprl/Spark-Input-Files/Customer-Data/customers-100000.csv")
df1=df.limit(1000)
print("\n\n\n")
df1.show()
print("\n\n\n")
#df1.write.mode("overwrite").format("csv").save("s3a://spark-test-aprl/Spark-Output/")