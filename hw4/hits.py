from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee
import re
import unicodedata

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()
    # .setMaster("local[*]") \
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = None

sc = spark.sparkContext
rdd = sc.textFile("graph.txt", partition)

links = rdd.map(lambda line:tuple(line.split())).map(lambda (node1, node2): (node1, [node2]))\
    .reduceByKey(add)

ranks = sc.parallelize(range(1,links.count()+1))

print(links.collect())