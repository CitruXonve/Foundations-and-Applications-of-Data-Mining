from pyspark.sql import SparkSession
from operator import add

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()
    # .setMaster("local[*]") \
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = 4

support = 100*1.0/partition

ratings = spark.read.csv("ratings.csv", 
                               header=True).rdd.map(tuple).repartition(partition)

baskets = ratings.map(lambda line: (int(line[0]), int(line[1]))).groupByKey().mapValues(frozenset)

# print(baskets.take(5))

candidates = ratings.map(lambda line: (int(line[1]), 1)).groupByKey().mapValues(len).filter(lambda x:x[1]>=support)

# print(candidates.take(5))

def mapper1(iterable):
    frequency = {}
    for x in iterable:
        item = x[1]
        frequency[item] = frequency.get(item, 0) + 1
    
    for item in frequency.items():
        if item[1]>=support:
            yield (item[0], 1)
    pass

def printer(iterable):
    for x in iterable:
        print x
    print("--")
    yield None

phase1 = ratings.mapPartitions(mapper1).reduceByKey(add)#.groupByKey().mapValues(list)

print(phase1.take(5))

print(baskets.getNumPartitions)