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

baskets = ratings.map(lambda line: (line[0], [line[1]])).reduceByKey(add)

print(baskets.take(5), baskets.values().getNumPartitions())

def mapper1(iterable):
    frequency = {}
    for basket in iterable:
        for item in basket:
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

phase1 = baskets.values().mapPartitions(mapper1).reduceByKey(add)

print(phase1.take(5), phase1.getNumPartitions(), phase1.keys())

phase1_broadcast = phase1.keys().broadcast()

def mapper2(iterable):
    frequency = {}

    def filter(key):
        for basket in iterable:
            basket_set = frozenset(basket)
            if key in basket_set:
                frequency[key] = frequency.get(key, 0) + 1
        
    phase1.keys().foreach(filter)
    for item in frequency.items():
        yield item
    pass

phase2 = baskets.values().mapPartitions(mapper2)

print(phase2.take(5))