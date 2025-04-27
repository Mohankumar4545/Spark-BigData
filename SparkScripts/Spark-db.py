from pyspark.sql import SparkSession 
from utils.SparkSession import SessionCreator
from utils.mysqlreader import MysqlConfig

appName= "MysqlDBConnect"
fileLoc="file:///home/mohankumar-r/Spark/SparkLearning/mysqlFile.txt"

spark=SessionCreator(appName)

#df=spark.read.format("csv").option("header","true").load(fileLoc)
#df.show()

# dfDB=spark.read.format("jdbc").\
#     option("url","jdbc:mysql://database-mysql.cjyqymgw6qbe.ap-southeast-2.rds.amazonaws.com/SparkTest").\
#     option("driver","com.mysql.cj.jdbc.Driver").\
#     option("dbtable","Persons").\
#     option("user","admin").\
#     option("password","Mohanrds45454545").\
#     load()

dfDB=MysqlConfig(spark)

dfDB.show()
#