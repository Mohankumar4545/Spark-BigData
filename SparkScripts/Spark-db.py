from pyspark.sql import SparkSession 
from utils.SparkSession import SessionCreator
from utils.mysqlreader import MysqlConfig

appName= "MysqlDBConnect"
fileLoc="file:///home/mohankumar-r/Spark/SparkLearning/mysqlFile.txt"

spark=SessionCreator(appName)

#df=spark.read.format("csv").option("header","true").load(fileLoc)
#df.show()



dfDB=MysqlConfig(spark)

dfDB.show()
#