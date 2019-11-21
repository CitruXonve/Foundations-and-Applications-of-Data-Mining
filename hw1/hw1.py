from pyspark.sql import SparkSession
from operator import add

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

# spark is an existing SparkSession
df_inspection = spark.read.csv("./la-restaurant-market-health-data/restaurant-and-market-health-inspections.csv", 
                               header=True)
df_violation = spark.read.csv("./la-restaurant-market-health-data/restaurant-and-market-health-violations.csv",
                             header=True)


rdd = df_inspection.rdd.map(tuple)
count = rdd.count()
q1 = rdd.map(lambda x:x[2]).distinct().count()
q2 = rdd.map(lambda x:x[3]).map(lambda x:(9 if int(x)>99 else int(x)/10,1)).reduceByKey(add) \
    .sortByKey().map(lambda x: '[90,100]:%d' % x[1] if x[0]>=9 else '[%d,%d]:%d' % (x[0], x[0]+9, x[1])).collect()
q3 = rdd.map(lambda x:x[3]).map(lambda x:(9 if int(x)>99 else int(x)/10,1)).reduceByKey(add) \
    .sortByKey(ascending=False).map(lambda x: 'A:%.2f' % (x[1]*100.0/count) if x[0]>=9 else '%c:%.2f' % (chr(ord('A')+9-x[0]), x[1]*100.0/count)).collect()
q4 = rdd.map(lambda x:x[10]).distinct().subtract(df_violation.rdd.map(lambda x:x[10]).distinct()).count()
answer = {}
answer['count'] = count
answer['a1'] = str(q1)
answer['a2'] = '\n'.join(q2)
answer['a3'] = '\n'.join(q3)
answer['a4'] = str(q4)

with open('q1.txt', 'w') as fout:
    fout.write(answer['a1'])

with open('q2.txt', 'w') as fout:
    fout.write(answer['a2'])

with open('q3.txt', 'w') as fout:
    fout.write(answer['a3'])

with open('q4.txt', 'w') as fout:
    fout.write(answer['a4'])