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
# q3 = rdd.map(lambda x:(x[4], 1)).reduceByKey(add) \
#     .sortByKey(ascending=False).map(lambda x: 'A:%.2f' % (x[1]*100.0/count) if x[0]>=9 else '%c:%.2f' % (chr(ord('A')+9-x[0]), x[1]*100.0/count)).collect()
q3_valid = rdd.map(lambda x:(x[4], 1)).filter(lambda x:x[0]>='A' and x[0]<='Z').reduceByKey(add).map(lambda line: '%s:%.2f' % (line[0], line[1]*100.0/count)).collect()
q3_invalid = rdd.map(lambda x:(x[4], 1)).filter(lambda x:x[0]<'A' or x[0]>'Z').reduceByKey(add).map(lambda line: '%s:%.2f' % (line[0], line[1]*100.0/count)).collect()


with open('Zongdi_Xu_q3.txt', 'w') as fout:
    fout.write('\n'.join(sorted(q3_valid)+sorted(q3_invalid)))