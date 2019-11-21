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
df_violation = spark.read.csv("restaurant-and-market-health-violations.csv",
                             header=True)


rdd = df_inspection.rdd.map(tuple)
count = rdd.count()
q4 = rdd.map(lambda x:x[10]).distinct().subtract(df_violation.rdd.map(lambda x:x[10]).distinct()).count()

with open('Zongdi_Xu_q4.txt', 'w') as fout:
    fout.write(str(q4))