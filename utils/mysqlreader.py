from dotenv import load_dotenv

import os

env_path = "/home/mohankumar-r/Spark/SparkLearning/params.env"
print(f"Loading .env from: {env_path}")
load_dotenv(dotenv_path=env_path, override=True)

def MysqlConfig(spark):
    return spark.read.format("jdbc").option("url",os.getenv("URL")).\
    option("driver",os.getenv("DRIVER")).\
    option("dbtable",os.getenv("DB")).\
    option("user",os.getenv("USER")).\
    option("password",os.getenv("PASSWORD")).load()
    

print("URL:", os.getenv("URL"))
print("DRIVER:", os.getenv("DRIVER"))
print("DB:", os.getenv("DB"))
print("USER:", os.getenv("USER"))
print("PASSWORD:", os.getenv("PASSWORD"))
