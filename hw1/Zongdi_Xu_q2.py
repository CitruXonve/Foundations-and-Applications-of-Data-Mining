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
q2 = rdd.map(lambda x:x[3]).map(lambda x:(9 if int(x)>99 else int(x)/10,1)).reduceByKey(add) \
    .sortByKey().map(lambda x: '[90,100]:%d' % x[1] if x[0]>=9 else '[%d,%d]:%d' % (x[0]*10, x[0]*10+9, x[1])).collect()

with open('Zongdi_Xu_q2.txt', 'w') as fout:
    fout.write('\n'.join(q2))