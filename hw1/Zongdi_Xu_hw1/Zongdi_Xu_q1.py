from pyspark.sql import SparkSession
from operator import add

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

# spark is an existing SparkSession
df_inspection = spark.read.csv("restaurant-and-market-health-inspections.csv", 
                               header=True)

rdd = df_inspection.rdd.map(tuple)
count = rdd.count()
q1 = rdd.map(lambda x:x[2]).distinct().count()

with open('Zongdi_Xu_q1.txt', 'w') as fout:
    fout.write(str(q1))