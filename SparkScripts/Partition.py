from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
import time
spark = SparkSession.builder \
    .appName("PartitionExample") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Create df1 with integer keys
df1 = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (8, "Charlie"),
    (9, "David")
], ["id", "name"])

# Create df2 with integer keys
df2 = spark.createDataFrame([
    (2, "Engineer"),
    (8, "Doctor"),
    (10, "Lawyer")
], ["id", "profession"])

# Repartition explicitly by 'id' to observe partitioning
df1_part = df1.repartition(4, "id").withColumn("partition", spark_partition_id())
df2_part = df2.repartition(4, "id").withColumn("partition", spark_partition_id())

print("DF1 partitions (integers):")
df1_part.show(truncate=False)

print("DF2 partitions (integers):")
df2_part.show(truncate=False)

# Perform a left join (on integer keys)
joined_df = df1_part.join(df2_part.drop("partition"), on="id", how="left")
joined_df.show(truncate=False)

time.sleep(2000)

